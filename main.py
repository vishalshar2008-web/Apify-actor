"""
Shopify Store Product & Pricing Feed Extractor - Apify Actor (Python + Crawlee + Playwright).

Extracts normalized product, pricing, variant, and image data from publicly accessible
Shopify storefronts and publishes ONE deduplicated dataset record per unique product,
with optional AI-friendly Markdown.

Key design points (per the technical specification):
  * Discovery: robots.txt sitemaps -> nested sitemap indexes -> collection pagination ->
    direct product links. Bounded by per-store page budgets and a global product cap.
  * Extraction tiers: (1) JSON-LD (application/ld+json, incl. @graph), (2) Shopify public
    product JSON embedded in rendered page scripts (e.g. `var meta = {"product": ...}`),
    (3) metadata / visible DOM (canonical, Open Graph, itemprop, headings, images),
    (4) partial extraction - missing fields stay null / [], never fabricated.
  * Safety: robots.txt respected, no bot-protection bypass (retry_on_blocked=False),
    per-domain rate-limit handling, SSRF-protected start URLs (private IPs rejected),
    no cart/checkout/account paths, no arbitrary internal link crawling.
  * Pricing: pay-per-event via Actor.push_data(charged_event_name="product-result")
    with ChargeResult inspection, plus legacy pay-per-result compatibility using
    Actor.configuration.max_paid_dataset_items and the existing dataset item count.
  * Money: Decimal arithmetic only, serialized as decimal strings, currency kept
    separate (null when unknown), compare-at price kept separate from selling price.
  * Errors: every step is wrapped in try/except and logged with the exact failing
    line in this file, the failing selector, and a truncated HTML snippet.

This is a public storefront extractor only. It does not authenticate, does not use the
Shopify Admin API, and does not attempt to bypass CAPTCHAs or access controls.
"""

from __future__ import annotations

import asyncio
import hashlib
import html as html_lib
import inspect
import ipaddress
import json
import logging
import re
import socket
import traceback
from pathlib import Path
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

from apify import Actor
from bs4 import BeautifulSoup
from crawlee import ConcurrencySettings, Request
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.errors import (
    HttpClientStatusCodeError,
    HttpStatusCodeError,
    PersistentRateLimitError,
    ProxyError,
    RequestThrottledError,
    SessionError,
)
from crawlee.request_loaders import ThrottlingRequestManager
from crawlee.storages import RequestQueue
from markdownify import MarkdownConverter

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------

SCHEMA_VERSION = '1.0'
RECORD_TYPE = 'shopify_product'
CHARGED_EVENT_NAME = 'product-result'  # Must match the monetization settings in Apify Console.

# Per-store caps so a huge store can never blow up the run.
MAX_SITEMAP_FILES_PER_STORE = 12
MAX_SITEMAP_DEPTH = 3
MAX_SITEMAP_PRODUCTS_PER_FILE = 1000
MAX_COLLECTIONS_FROM_SITEMAP = 10
MAX_HOMEPAGE_DIRECT_PRODUCTS = 30
MAX_HOMEPAGE_DIRECT_COLLECTIONS = 10
MAX_DOM_FALLBACK_IMAGES = 12
HTML_SNIPPET_LIMIT = 600  # chars of flattened HTML included in error logs.
GLOBAL_PAGE_BUDGET_SLACK = 15

# Query parameters that are safe to strip (tracking). `page` is NEVER stripped.
TRACKING_PARAM_PREFIXES = ('utm_', 'hsa_', 'pk_', 'matomo_')
TRACKING_PARAM_NAMES = {
    'fbclid', 'gclid', 'dclid', 'msclkid', 'mc_cid', 'mc_eid', 'igshid', 'igsh',
    'ref', 'ref_src', 'srsltid', '_ga', '_gl', 'yclid', 'ttclid', 'twclid', 'li_fat_id',
}

# Path segments that must never be enqueued (checkout/cart/account etc. or out of scope).
BLOCKED_PATH_SEGMENTS = frozenset({
    'cart', 'checkout', 'checkouts', 'account', 'accounts', 'login', 'logout', 'register',
    'signin', 'signup', 'customers', 'orders', 'search', 'password', 'pages', 'blogs',
    'policies', 'policy', 'legal', 'tools', 'apps', 'downloads', 'cdn-cgi', 'wallets',
    'recommendations', 'admin', 'wp-admin', 'cgi-bin', 'oauth', 'challenge', 'vote',
    'set_currency', 'compare', 'community', 'forums', 'galleries',
})
BLOCKED_PATH_SUBSTRINGS = ('/recommendations/', '/cdn-cgi/')

# Asset-looking paths (only filtered for links discovered inside HTML pages).
ASSET_EXT_RE = re.compile(
    r'\.(css|js|mjs|json|xml|png|jpe?g|gif|webp|avif|svg|ico|bmp|woff2?|ttf|otf|eot'
    r'|mp3|mp4|webm|mov|pdf|zip|gz|tgz|csv|xls|xlsx|doc|docx)(\?|$)',
    re.IGNORECASE,
)

# Optional locale prefix, e.g. /en-us/products/x, /fr/collections/y, /es-419/...
LOCALE_PREFIX = r'(?:/[a-z]{2,3}(?:-[a-z0-9]{2,8})?)?'
PRODUCT_PATH_RE = re.compile(rf'{LOCALE_PREFIX}/products/([^/?#]+)', re.IGNORECASE)
COLLECTION_PATH_RE = re.compile(rf'{LOCALE_PREFIX}/collections/([a-z0-9][a-z0-9_\-]*)/?$', re.IGNORECASE)

SHOPIFY_MARKERS = (
    'cdn.shopify.com', '/cdn/shop/', 'var shopify', 'window.shopify', 'shopify.theme',
    'shopify-features', 'web-pixels-manager', 'shopify-digital-wallet', 'myshopify.com',
    'shopify-analytics', 'shopifycdn', 'shopify.locale', 'shopify.currency',
)

CAPTCHA_MARKERS = (
    'cf-chl', 'cf_chl_', 'challenge-platform', 'g-recaptcha', 'h-captcha',
    'hcaptcha.com', 'px-captcha', 'perimeterx', '_incapsula', 'datadome',
    'are you a robot', 'verify you are human', 'checking your browser',
    'just a moment...', 'attention required! | cloudflare',
)

NEXT_PAGE_TEXT_RE = re.compile(r'^(next|next page|older|next »|›|»|>)$', re.IGNORECASE)

# --------------------------------------------------------------------------------------
# Error logging helpers (the "step failed -> exact line + HTML snippet" machinery)
# --------------------------------------------------------------------------------------


def _html_snippet(html: str | None, limit: int = HTML_SNIPPET_LIMIT) -> str:
    """Return a whitespace-flattened HTML snippet safe for log lines."""
    if not html:
        return '<no-html>'
    flat = ' '.join(html.split())
    if len(flat) <= limit:
        return flat
    return f'{flat[:limit]} ...[truncated, {len(flat) - limit} more chars]'


def _caller_location(skip: int = 1) -> str:
    """Return 'function:lineno' of the calling frame (used for selector-miss logs)."""
    frame = inspect.stack()[skip]
    return f'{frame.function}:{frame.lineno}'


def log_step_error(
    step: str,
    exc: BaseException,
    *,
    url: str = '',
    selector: str | None = None,
    html: str | None = None,
    tag: str = 'PARSE_ERROR',
    log: logging.Logger | None = None,
) -> None:
    """Log a failed step with the exact source line that raised and an HTML snippet."""
    logger = log or Actor.log
    try:
        frames = traceback.extract_tb(exc.__traceback__)
        # Prefer the last frame from this file - it names the exact failing line in main.py.
        this_file = Path(__file__).name
        frame = next((f for f in reversed(frames) if Path(f.filename).name == this_file), frames[-1])
        where = f'{Path(frame.filename).name}:{frame.lineno} in {frame.name}'
        source_line = (frame.line or '').strip()
    except Exception:
        where, source_line = _caller_location(2), ''
    parts = [f'[{tag}] step={step!r}', f'where={where}']
    if source_line:
        parts.append(f'raised_by={source_line[:200]!r}')
    if url:
        parts.append(f'url={url}')
    if selector is not None:
        parts.append(f'selector={selector!r}')
    parts.append(f'error={type(exc).__name__}: {exc}')
    if html:
        parts.append(f'html_snippet={_html_snippet(html)}')
    logger.error(' | '.join(parts))


def log_selector_miss(
    step: str,
    *,
    url: str = '',
    selector: str = '',
    found: int = 0,
    html: str | None = None,
    extra: str = '',
    log: logging.Logger | None = None,
) -> None:
    """Log a selector that matched nothing, including where it was used + an HTML snippet."""
    logger = log or Actor.log
    parts = [
        f'[SELECTOR_MISS] step={step!r}',
        f'where={_caller_location(2)}',
        f'selector={selector!r}',
        f'found={found}',
    ]
    if url:
        parts.append(f'url={url}')
    if extra:
        parts.append(str(extra))
    if html:
        parts.append(f'html_snippet={_html_snippet(html, 350)}')
    logger.warning(' | '.join(parts))


def _run_step(step: str, fn: Callable[[], Any], *, url: str = '', html: str | None = None,
              default: Any = None, log: logging.Logger | None = None) -> Any:
    """Run an extraction step; on failure log it with line + snippet and return `default`."""
    try:
        return fn()
    except Exception as exc:
        log_step_error(step, exc, url=url, html=html, log=log)
        return default


# --------------------------------------------------------------------------------------
# Small normalization helpers (text, time, prices)
# --------------------------------------------------------------------------------------


def clean_text(value: Any) -> str:
    """Collapse whitespace and trim. Never raises; non-strings are stringified."""
    if value is None:
        return ''
    if not isinstance(value, str):
        value = str(value)
    return re.sub(r'\s+', ' ', value).strip()


def iso_utc(value: Any) -> str | None:
    """Normalize a timestamp string to UTC ISO 8601 with a trailing Z. None when unparsable."""
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    if s.endswith(('Z', 'z')):
        s = f'{s[:-1]}+00:00'
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def normalize_price(value: Any, *, cents: bool = False) -> Decimal | None:
    """Convert a raw price value to a Decimal. NEVER uses binary float arithmetic.

    Handles: None, int (optionally minor units when `cents=True`), float (via str()),
    numeric strings possibly with currency symbols/commas, and dicts with an
    `amount`/`price` key (Shopify's newer storefront JSON format).
    Returns None for anything unusable - a missing price is never zero.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, dict):
        for key in ('amount', 'price'):
            if key in value:
                # Dict amounts are already in major units (e.g. "49.00").
                return normalize_price(value[key], cents=False)
        return None
    try:
        if isinstance(value, int):
            dec = Decimal(value)
        elif isinstance(value, float):
            dec = Decimal(str(value))
        elif isinstance(value, Decimal):
            dec = value
        elif isinstance(value, str):
            cleaned = re.sub(r'[^0-9.\-]', '', value.replace(',', '').strip())
            if not cleaned or cleaned in ('-', '.', '-.'):
                return None
            dec = Decimal(cleaned)
        else:
            return None
    except (InvalidOperation, ArithmeticError, ValueError):
        return None
    if cents:
        dec = dec / Decimal(100)
    return dec


def money_str(value: Decimal | None) -> str | None:
    """Serialize money as a 2-decimal string ("49.99"). None stays None."""
    if value is None:
        return None
    try:
        return format(value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP), 'f')
    except InvalidOperation:
        return None


def looks_like_currency(value: Any) -> bool:
    return isinstance(value, str) and bool(re.fullmatch(r'[A-Z]{3}', value.strip()))


# --------------------------------------------------------------------------------------
# URL / domain / SSRF helpers
# --------------------------------------------------------------------------------------


def normalize_domain(host: str | None) -> str | None:
    """Lowercase a hostname, strip the trailing dot and any leading 'www.'."""
    if not host:
        return None
    host = host.strip().lower().rstrip('.')
    if host.startswith('www.'):
        host = host[4:]
    return host or None


def strip_tracking_params(query: str) -> str:
    """Drop tracking params, keep meaningful ones (notably `page`) in original order."""
    kept = []
    for key, val in parse_qsl(query, keep_blank_values=True):
        low = key.lower()
        if low in TRACKING_PARAM_NAMES or any(low.startswith(p) for p in TRACKING_PARAM_PREFIXES):
            continue
        kept.append((key, val))
    return urlencode(kept)


def normalize_url(url: str | None, *, base: str | None = None) -> str | None:
    """Canonicalize a URL for enqueueing/dedup. Returns None for anything non-HTTP(S)."""
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if url.startswith('//'):
        url = f'https:{url}'
    if base:
        url = urljoin(base, url)
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if parts.scheme.lower() not in ('http', 'https'):
        return None
    host = (parts.hostname or '').lower().rstrip('.')
    if not host:
        return None
    try:
        port = parts.port
    except ValueError:
        return None
    default_port = 80 if parts.scheme.lower() == 'http' else 443
    netloc = host if port in (None, default_port) else f'{host}:{port}'
    path = parts.path or '/'
    if len(path) > 1 and path.endswith('/'):
        path = path.rstrip('/') or '/'
    query = strip_tracking_params(parts.query)
    return urlunsplit((parts.scheme.lower(), netloc, path, query, ''))


def _ip_is_public(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Reject loopback/private/link-local/reserved/multicast/unspecified addresses."""
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def hostname_is_public(host: str, *, resolve: bool = True) -> tuple[bool, str]:
    """SSRF guard. Checks the hostname shape and (optionally) that DNS resolves publicly."""
    h = (host or '').strip().lower().rstrip('.')
    if not h:
        return False, 'empty host'
    if h in ('localhost', 'ip6-localhost', 'ip6-loopback'):
        return False, 'localhost is not allowed'
    if '.' not in h and ':' not in h:
        return False, 'hostname without a dot is not allowed'
    if h.endswith(('.local', '.internal', '.localdomain', '.lan', '.home', '.corp', '.intranet')):
        return False, 'internal suffix is not allowed'
    try:
        literal = ipaddress.ip_address(h)
    except ValueError:
        pass
    else:
        if not _ip_is_public(literal):
            return False, f'non-public IP literal {h}'
        return True, 'public IP literal'
    if not resolve:
        return True, 'public hostname (DNS not checked)'
    try:
        infos = socket.getaddrinfo(h, None)
    except OSError as exc:
        return False, f'DNS resolution failed: {exc}'
    for _, _, _, _, sockaddr in infos:
        try:
            ip = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            continue
        if not _ip_is_public(ip):
            return False, f'host resolves to non-public IP {sockaddr[0]}'
    return True, 'public hostname'


def classify_url_path(path: str) -> tuple[str, str | None]:
    """Classify a URL path as ('product'|'collection'|'homepage', handle)."""
    match = PRODUCT_PATH_RE.search(path or '')
    if match:
        handle = match.group(1)
        if handle.lower() not in ('.js', '.json'):
            return 'product', handle
    if COLLECTION_PATH_RE.search(path or ''):
        return 'collection', COLLECTION_PATH_RE.search(path).group(1)  # type: ignore[union-attr]
    return 'homepage', None


def path_is_blocked(path: str) -> bool:
    """True for checkout/cart/account/... or out-of-scope paths (pages, blogs, search...)."""
    if not path:
        return False
    lowered = path.lower()
    for substring in BLOCKED_PATH_SUBSTRINGS:
        if substring in lowered:
            return True
    segments = [seg for seg in lowered.split('/') if seg]
    for segment in segments[:2]:  # first two segments (covers /fr/cart style locales too)
        if segment in BLOCKED_PATH_SEGMENTS:
            return True
    return False


def looks_like_shopify(html: str) -> bool:
    """Theme-independent Shopify storefront markers (checked on rendered HTML)."""
    lowered = html.lower()
    return any(marker in lowered for marker in SHOPIFY_MARKERS)


def looks_like_captcha(html: str) -> bool:
    """Detect bot-challenge / CAPTCHA pages (treated as non-recoverable access denial)."""
    head = html[:8000].lower()
    return any(marker in head for marker in CAPTCHA_MARKERS)


# --------------------------------------------------------------------------------------
# Input validation and crawl configuration
# --------------------------------------------------------------------------------------


class InputValidationError(ValueError):
    """Raised when the Actor input is unusable (no valid start URL, bad limits, ...)."""


@dataclass(frozen=True)
class StartTarget:
    """One validated, normalized starting URL."""
    url: str
    host: str  # exact (lowercased) hostname, e.g. 'www.allbirds.com'
    store_domain: str  # www-stripped domain used for budgets and product keys
    kind: str  # 'homepage' | 'collection' | 'product'


@dataclass
class CrawlConfig:
    targets: list[StartTarget]
    allowed_hosts: set[str]
    store_domains: set[str]
    max_products: int
    max_pages_per_store: int
    max_concurrency: int
    max_tasks_per_minute: int
    include_variants: bool
    include_markdown: bool

    @property
    def max_requests_per_crawl(self) -> int:
        """Global guardrail computed from per-store caps and the store count."""
        return self.max_pages_per_store * max(1, len(self.store_domains)) + GLOBAL_PAGE_BUDGET_SLACK


def _clamp_int(name: str, value: Any, *, default: int, minimum: int, maximum: int) -> int:
    """Coerce a numeric input to int within schema bounds; invalid values fall back."""
    if value is None:
        return default
    try:
        converted = int(value)
    except (TypeError, ValueError):
        Actor.log.warning(f'[VALIDATION] field={name!r} value={value!r} is not an integer, using default {default}')
        return default
    if converted < minimum or converted > maximum:
        clamped = max(minimum, min(maximum, converted))
        Actor.log.warning(f'[VALIDATION] field={name!r} value={converted} out of [{minimum}, {maximum}], clamped to {clamped}')
        return clamped
    return converted


def _as_bool(name: str, value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ('true', '1', 'yes', 'on'):
        return True
    if isinstance(value, str) and value.strip().lower() in ('false', '0', 'no', 'off'):
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    Actor.log.warning(f'[VALIDATION] field={name!r} value={value!r} is not a boolean, using default {default}')
    return default


def validate_input(raw_input: dict[str, Any]) -> CrawlConfig:
    """Validate the Actor input per the spec: schemes, SSRF, normalization, limits.

    Raises InputValidationError when no usable start URL remains.
    """
    if not isinstance(raw_input, dict):
        raise InputValidationError('Actor input must be a JSON object.')

    raw_urls = raw_input.get('startUrls')
    if not isinstance(raw_urls, list) or not raw_urls:
        raise InputValidationError('At least one start URL is required (startUrls).')

    candidates: list[str] = []
    for entry in raw_urls:
        raw_url = entry.get('url') if isinstance(entry, dict) else entry
        if isinstance(raw_url, str) and raw_url.strip():
            candidates.append(raw_url.strip())
    if not candidates:
        raise InputValidationError('startUrls did not contain any usable "url" values.')

    errors: list[str] = []
    targets_by_url: dict[str, StartTarget] = {}

    for raw_url in candidates:
        normalized = normalize_url(raw_url)
        if normalized is None:
            errors.append(f'rejected {raw_url!r}: only absolute http(s) URLs are supported')
            continue
        parts = urlsplit(normalized)
        host = parts.hostname or ''
        # SSRF guard: reject localhost / private / link-local / non-public targets.
        public_ok, reason = hostname_is_public(host, resolve=True)
        if not public_ok:
            errors.append(f'rejected {raw_url!r}: {reason}')
            continue
        kind, _handle = classify_url_path(parts.path)
        target = StartTarget(
            url=normalized,
            host=host,
            store_domain=normalize_domain(host) or host,
            kind=kind,
        )
        if normalized not in targets_by_url:
            targets_by_url[normalized] = target  # dedupe identical normalized start URLs

    if not targets_by_url:
        raise InputValidationError(f'No valid public start URL remained. {"; ".join(errors)}')

    for message in errors:
        Actor.log.warning(f'[VALIDATION] {message}')

    targets = list(targets_by_url.values())
    allowed_hosts: set[str] = set()
    for target in targets:
        allowed_hosts.add(target.host)
        if not target.host.startswith('www.'):
            allowed_hosts.add(f'www.{target.host}')
        else:
            allowed_hosts.add(target.host[4:])

    config = CrawlConfig(
        targets=targets,
        allowed_hosts=allowed_hosts,
        store_domains={t.store_domain for t in targets},
        max_products=_clamp_int('maxProducts', raw_input.get('maxProducts'), default=1000, minimum=1, maximum=10000),
        max_pages_per_store=_clamp_int('maxPagesPerStore', raw_input.get('maxPagesPerStore'), default=100, minimum=1, maximum=5000),
        max_concurrency=_clamp_int('maxConcurrency', raw_input.get('maxConcurrency'), default=2, minimum=1, maximum=3),
        max_tasks_per_minute=_clamp_int('maxTasksPerMinute', raw_input.get('maxTasksPerMinute'), default=20, minimum=1, maximum=120),
        include_variants=_as_bool('includeVariants', raw_input.get('includeVariants'), default=True),
        include_markdown=_as_bool('includeMarkdown', raw_input.get('includeMarkdown'), default=True),
    )
    Actor.log.info(
        '[VALIDATION] accepted start targets: '
        + ', '.join(f'{t.kind}:{t.url}' for t in config.targets)
    )
    return config


# --------------------------------------------------------------------------------------
# Mutable crawl state (budgets, dedup sets, counters)
# --------------------------------------------------------------------------------------


@dataclass
class CrawlState:
    visited_urls: set[str] = field(default_factory=set)
    published_keys: set[str] = field(default_factory=set)
    pages_used: Counter = field(default_factory=Counter)  # store_domain -> processed pages
    pages_pending: Counter = field(default_factory=Counter)
    products_pending: Counter = field(default_factory=Counter)
    sitemap_files_used: Counter = field(default_factory=Counter)
    restricted_domains: dict[str, str] = field(default_factory=dict)  # store_domain -> reason
    published: int = 0
    duplicates: int = 0
    parse_errors: int = 0
    nav_errors: int = 0
    not_found: int = 0
    product_pages_processed: int = 0
    robots_skipped: int = 0
    products_limit_hit: bool = False
    charge_limit_hit: bool = False
    limit_stop_logged: bool = False
    publish_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def restrict(self, store_domain: str, reason: str) -> None:
        self.restricted_domains.setdefault(store_domain, reason)

    def is_restricted(self, store_domain: str) -> bool:
        return store_domain in self.restricted_domains


# --------------------------------------------------------------------------------------
# Tier 1: JSON-LD (script[type="application/ld+json"]) parsing
# --------------------------------------------------------------------------------------

JSON_LD_SELECTOR = 'script[type="application/ld+json"]'


def _ld_walk_nodes(node: Any) -> Iterable[dict]:
    """Yield every dict node in a JSON-LD structure, recursing into @graph/mainEntity."""
    if isinstance(node, list):
        for item in node:
            yield from _ld_walk_nodes(item)
    elif isinstance(node, dict):
        yield node
        for key in ('@graph', 'mainEntity', 'mainEntityOfPage'):
            child = node.get(key)
            if isinstance(child, (dict, list)):
                yield from _ld_walk_nodes(child)


def _ld_type_is(node: dict, type_name: str) -> bool:
    raw_type = node.get('@type')
    types = raw_type if isinstance(raw_type, list) else [raw_type]
    return any(isinstance(t, str) and t.strip().lower() == type_name for t in types)


def collect_ld_products(soup: BeautifulSoup, *, url: str = '', log: logging.Logger | None = None) -> list[dict]:
    """Parse all JSON-LD blocks and return every schema.org Product node (incl. @graph)."""
    logger = log or Actor.log
    scripts = soup.find_all('script', type=re.compile(r'application/ld\+json', re.IGNORECASE))
    if not scripts:
        log_selector_miss('json_ld.find_blocks', url=url, selector=JSON_LD_SELECTOR, html=str(soup)[:4000], log=logger)
        return []
    products: list[dict] = []
    for script in scripts:
        raw = script.string if isinstance(script.string, str) else script.get_text()
        if not raw or not raw.strip():
            continue
        try:
            parsed = json.loads(raw)
        except Exception as exc:
            # Malformed JSON-LD must never crash the crawl - log and continue.
            log_step_error('json_ld.parse', exc, url=url, selector=JSON_LD_SELECTOR, html=raw, tag='PARSE_ERROR', log=logger)
            continue
        for node in _ld_walk_nodes(parsed):
            if isinstance(node, dict) and _ld_type_is(node, 'product'):
                products.append(node)
    return products


def pick_main_ld_product(products: list[dict], *, final_url: str, og_title: str | None) -> dict | None:
    """Pick the product node matching this page (URL first, then og:title), else the first."""
    if not products:
        return None
    normalized_final = normalize_url(final_url)
    for product in products:
        candidate = normalize_url(str(product.get('url', '')) or None)
        if candidate and candidate == normalized_final:
            return product
    if og_title:
        for product in products:
            if clean_text(product.get('name')) == clean_text(og_title):
                return product
    return products[0]


def _ld_images(value: Any) -> list[str]:
    """Flatten a JSON-LD `image` value (str | dict | list of either) into URL strings."""
    out: list[str] = []

    def one(item: Any) -> None:
        if isinstance(item, str) and item.strip():
            out.append(item.strip())
        elif isinstance(item, dict):
            for key in ('url', 'contentUrl', '@id'):
                inner = item.get(key)
                if isinstance(inner, str) and inner.strip():
                    out.append(inner.strip())
                    break

    if isinstance(value, list):
        for item in value:
            one(item)
    else:
        one(value)
    return out


def _ld_brand(value: Any) -> str | None:
    if isinstance(value, str):
        return clean_text(value) or None
    if isinstance(value, dict):
        return clean_text(value.get('name')) or None
    return None


def parse_ld_offers(offers_raw: Any) -> dict[str, Any]:
    """Normalize Offer / AggregateOffer structures (dict, list, or nested)."""
    result: dict[str, Any] = {'prices': [], 'low': None, 'high': None, 'currency': None, 'availability': []}

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        raw_type = node.get('@type')
        type_str = raw_type if isinstance(raw_type, str) else ','.join(t for t in raw_type if isinstance(t, str)) if isinstance(raw_type, list) else ''
        lowered = type_str.lower()
        if isinstance(node.get('availability'), str):
            result['availability'].append(node['availability'])
        currency = node.get('priceCurrency')
        if looks_like_currency(currency) and result['currency'] is None:
            result['currency'] = currency.strip()
        if 'aggregateoffer' in lowered:
            low = normalize_price(node.get('lowPrice'))
            high = normalize_price(node.get('highPrice'))
            if low is not None:
                result['low'] = low if result['low'] is None else min(result['low'], low)
            if high is not None:
                result['high'] = high if result['high'] is None else max(result['high'], high)
            price_direct = normalize_price(node.get('price'))
            if price_direct is not None:
                result['prices'].append(price_direct)
            walk(node.get('offers'))
        elif 'offer' in lowered:
            price = normalize_price(node.get('price'))
            if price is not None:
                result['prices'].append(price)
            walk(node.get('offers'))

    walk(offers_raw)
    return result


def availability_from_strings(strings: Sequence[str]) -> bool | None:
    """Map schema.org availability URLs to True/False/None (unknown stays None)."""
    if not strings:
        return None
    lowered = [s.strip().lower() for s in strings]
    if any('instock' in s or 'in-stock' in s or 'preorder' in s for s in lowered):
        return True
    if all('outofstock' in s or 'out-of-stock' in s or 'soldout' in s or 'discontinued' in s for s in lowered):
        return False
    return None


def extract_ld_product_fields(product: dict) -> dict[str, Any]:
    """Build a field-bag fragment from a schema.org Product JSON-LD node."""
    fragment: dict[str, Any] = {}
    fragment['title'] = clean_text(product.get('name')) or None
    description = product.get('description')
    if isinstance(description, str) and description.strip():
        fragment['description_html'] = description
    fragment['vendor'] = _ld_brand(product.get('brand') or product.get('manufacturer'))
    fragment['product_type'] = clean_text(product.get('category')) or None
    fragment['sku'] = clean_text(product.get('sku')) or None
    gtin = next((product.get(k) for k in ('gtin13', 'gtin12', 'gtin14', 'gtin8', 'gtin') if product.get(k)), None)
    fragment['gtin'] = clean_text(gtin) or None
    product_id = product.get('productID')
    if isinstance(product_id, (str, int)) and not isinstance(product_id, bool):
        fragment['product_id'] = str(product_id)
    ld_url = normalize_url(str(product.get('url', '')) or None)
    if ld_url:
        fragment['canonical'] = ld_url
    images = _ld_images(product.get('image'))
    if images:
        fragment['images'] = images
    offers = parse_ld_offers(product.get('offers'))
    fragment['ld_offers'] = offers
    if offers['currency']:
        fragment['currency'] = offers['currency']
    available = availability_from_strings(offers['availability'])
    if available is not None:
        fragment['available'] = available
    published = product.get('datePublished')
    if isinstance(published, str):
        fragment['published_at'] = iso_utc(published)
    keywords = product.get('keywords')
    if isinstance(keywords, str) and keywords.strip():
        fragment['tags'] = [t.strip() for t in keywords.split(',') if t.strip()]
    return fragment


# --------------------------------------------------------------------------------------
# Tier 2: Shopify public product JSON embedded in rendered page scripts
# --------------------------------------------------------------------------------------

META_VARIANTS_RE = re.compile(r'"variants"\s*:')
META_PRODUCT_SOURCES = (re.compile(r'\{"product"\s*:'), re.compile(r'\{\\"product\\"\s*:'), re.compile(r'var\s+meta\s*=\s*'))


def _balanced_json_from(text: str, start: int) -> str | None:
    """Extract a brace-balanced JSON object string starting at text[start] == '{'.

    Respects JSON string quoting and escapes so embedded/escaped objects are handled.
    """
    if start < 0 or start >= len(text) or text[start] != '{':
        return None
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, min(len(text), start + 2_000_000)):
        char = text[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char == '{':
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                return text[start:index + 1]
    return None


def find_shopify_product_json(soup: BeautifulSoup, *, url: str = '', log: logging.Logger | None = None) -> dict | None:
    """Locate a Shopify product JSON object embedded in page scripts.

    Covers `var meta = {"product": ...}` and (escaped) `{"product": ...}` objects inside
    modern web-pixels setup scripts. Requires variants + handle/title to qualify.
    """
    logger = log or Actor.log
    for script in soup.find_all('script'):
        raw = script.string if isinstance(script.string, str) else script.get_text()
        if not raw or len(raw) < 40 or not META_VARIANTS_RE.search(raw):
            continue
        starts: list[int] = []
        for pattern in META_PRODUCT_SOURCES:
            for match in pattern.finditer(raw):
                brace = raw.find('{', match.end())
                if brace != -1:
                    starts.append(brace)
        for start in starts:
            blob = _balanced_json_from(raw, start)
            if not blob:
                continue
            data = None
            for probe in (blob, blob.replace('\\"', '"')):
                try:
                    data = json.loads(probe)
                    break
                except Exception:
                    continue
            if not isinstance(data, dict):
                continue
            candidate = data.get('product') if isinstance(data.get('product'), dict) else data
            if (
                isinstance(candidate, dict)
                and isinstance(candidate.get('variants'), list)
                and ('handle' in candidate or 'title' in candidate)
            ):
                return candidate
    log_selector_miss(
        'shopify_meta.find_product_json', url=url,
        selector='script text matching {"product": ...} / var meta = ...',
        html=str(soup)[:4000], log=logger,
    )
    return None


def normalize_options(product_json: dict) -> list[dict]:
    """Normalize Shopify product `options` into [{"name": ..., "values": [...]}]."""
    raw_options = product_json.get('options')
    out: list[dict] = []
    if not isinstance(raw_options, list):
        return out
    for option in raw_options:
        if isinstance(option, dict) and clean_text(option.get('name')):
            values = option.get('values') or []
            out.append({
                'name': clean_text(option['name']),
                'values': [clean_text(v) for v in values if clean_text(v)],
            })
        elif isinstance(option, str) and option.strip():
            out.append({'name': option.strip(), 'values': []})
    return out


def normalize_variant(variant: Any, option_specs: list[dict]) -> dict | None:
    """Normalize one Shopify variant into the output contract shape.

    Integer prices are Shopify's documented minor units (cents) and are converted
    with Decimal arithmetic; dict prices ({amount, currency_code}) use the amount.
    """
    if not isinstance(variant, dict):
        return None
    raw_price = variant.get('price')
    price = normalize_price(raw_price, cents=isinstance(raw_price, int) and not isinstance(raw_price, bool))
    raw_compare = variant.get('compare_at_price')
    compare = normalize_price(raw_compare, cents=isinstance(raw_compare, int) and not isinstance(raw_compare, bool))
    options_map: dict[str, str] = {}
    if option_specs:
        named_keys = [option['name'] for option in option_specs]
        if any(f'option{idx}' in variant for idx in (1, 2, 3)):
            for idx in range(1, len(named_keys) + 1):
                value = variant.get(f'option{idx}')
                name = named_keys[idx - 1] if idx - 1 < len(named_keys) else f'Option {idx}'
                if clean_text(value) and name:
                    options_map[name] = clean_text(value)
        elif isinstance(variant.get('options'), list):
            for name, value in zip(named_keys, variant['options']):
                if clean_text(value) and name:
                    options_map[name] = clean_text(value)
    raw_id = variant.get('id')
    variant_id = str(raw_id) if isinstance(raw_id, (int, str)) and not isinstance(raw_id, bool) else None
    available = variant.get('available')
    dedup_key = variant_id
    if dedup_key is None:
        dedup_key = '|'.join(
            [str(price or ''), str(compare or '')] + [f'{k}={v}' for k, v in sorted(options_map.items())]
        )
    title = clean_text(variant.get('public_title') or variant.get('title')) or None
    return {
        'variant_id': variant_id,
        'title': title,
        'sku': clean_text(variant.get('sku')) or None,
        'barcode': clean_text(variant.get('barcode') or variant.get('gtin')) or None,
        'available': available if isinstance(available, bool) else None,
        'price': price,  # Decimal | None -> money_str at record build
        'compare_at_price': compare,
        'options': options_map,
        '_dedup_key': dedup_key,
    }


def extract_shopify_meta_fields(product_json: dict) -> dict[str, Any]:
    """Build a field-bag fragment from the embedded Shopify product JSON object."""
    fragment: dict[str, Any] = {}
    fragment['title'] = clean_text(product_json.get('title')) or None
    fragment['handle'] = clean_text(product_json.get('handle')) or None
    raw_id = product_json.get('id')
    if isinstance(raw_id, (int, str)) and not isinstance(raw_id, bool):
        fragment['product_id'] = str(raw_id)
    fragment['vendor'] = clean_text(product_json.get('vendor')) or None
    fragment['product_type'] = clean_text(product_json.get('product_type')) or None
    fragment['published_at'] = iso_utc(product_json.get('published_at'))
    fragment['available'] = product_json.get('available') if isinstance(product_json.get('available'), bool) else None
    currency = product_json.get('currency')
    if looks_like_currency(currency):
        fragment['currency'] = currency.strip()
    tags = product_json.get('tags')
    fragment['tags'] = [clean_text(t) for t in tags if clean_text(t)] if isinstance(tags, list) else []

    description = product_json.get('body_html') or product_json.get('description') or product_json.get('descriptionHtml')
    if isinstance(description, str) and description.strip():
        fragment['description_html'] = description

    images: list[str] = []

    def _image_entry(item: Any) -> None:
        if isinstance(item, str) and item.strip():
            images.append(item.strip())
        elif isinstance(item, dict):
            src = item.get('src') or item.get('url') or item.get('source')
            if isinstance(src, str) and src.strip():
                images.append(src.strip())

    _image_entry(product_json.get('featured_image'))
    for item in product_json.get('images') or []:
        _image_entry(item)
    if images:
        fragment['images'] = images

    fragment['options'] = normalize_options(product_json)

    variants_out: list[dict] = []
    seen_variant_keys: set[str] = set()
    for raw_variant in product_json.get('variants') or []:
        normalized = normalize_variant(raw_variant, fragment['options'])
        if not normalized:
            continue
        # Deduplicate by public variant ID (or the conservative composite key).
        if normalized['_dedup_key'] in seen_variant_keys:
            continue
        seen_variant_keys.add(normalized['_dedup_key'])
        variants_out.append(normalized)
    fragment['variants'] = variants_out

    # Product-level price bounds, when the source provides them (minor units).
    price_min = normalize_price(product_json.get('price_min'), cents=isinstance(product_json.get('price_min'), int))
    price_max = normalize_price(product_json.get('price_max'), cents=isinstance(product_json.get('price_max'), int))
    if price_min is not None or price_max is not None:
        fragment['product_price_bounds'] = (price_min, price_max)
    return fragment


# --------------------------------------------------------------------------------------
# Tier 3: metadata and visible DOM extraction (theme-independent selectors)
# --------------------------------------------------------------------------------------


def _meta_content(soup: BeautifulSoup, selector: str, attribute: str = 'content') -> str | None:
    """Look up a meta tag by several attribute patterns and return its content."""
    try:
        for attr_name in ('property', 'name', 'itemprop'):
            candidates = soup.select(f'meta[{attr_name}="{selector}"]')
            if candidates:
                value = candidates[0].get(attribute) or candidates[0].get_text(strip=True)
                if isinstance(value, str) and value.strip():
                    return value.strip()
    except Exception:
        # `select` with weird selector strings must never crash extraction.
        return None
    return None


def extract_dom_fields(soup: BeautifulSoup, *, final_url: str, log: logging.Logger | None = None) -> dict[str, Any]:
    """Build a field-bag fragment from canonical/OG/itemprop metadata and the visible DOM."""
    logger = log or Actor.log
    fragment: dict[str, Any] = {}

    fragment['og_title'] = clean_text(_meta_content(soup, 'og:title')) or None
    fragment['og_description'] = clean_text(_meta_content(soup, 'og:description')) or None
    og_url = normalize_url(_meta_content(soup, 'og:url'), base=final_url)
    if og_url:
        fragment['og_url'] = og_url

    canonical = None
    link = _run_step(
        'dom.canonical',
        lambda: soup.find('link', rel=lambda v: v and 'canonical' in v),
        url=final_url, log=logger,
    )
    if link is not None:
        canonical = normalize_url(link.get('href'), base=final_url)
    if canonical:
        fragment['canonical'] = canonical

    # Price/currency metadata (theme-independent semantic selectors).
    price_meta = (
        _meta_content(soup, 'product:price:amount')
        or _meta_content(soup, 'og:price:amount')
        or _meta_content(soup, 'price')
    )
    price = normalize_price(price_meta)
    if price is not None:
        fragment['price'] = price
    currency_meta = (
        _meta_content(soup, 'product:price:currency')
        or _meta_content(soup, 'og:price:currency')
        or _meta_content(soup, 'priceCurrency')
    )
    if looks_like_currency(currency_meta):
        fragment['currency'] = currency_meta.strip()

    # Images: Open Graph + itemprop image + rel=image_src.
    images: list[str] = []
    for key in ('og:image', 'og:image:secure_url', 'image'):
        value = _meta_content(soup, key)
        if value:
            images.append(value)
    link_img = _run_step(
        'dom.image_src', lambda: soup.find('link', rel=lambda v: v and 'image_src' in v),
        url=final_url, log=logger,
    )
    if link_img is not None and isinstance(link_img.get('href'), str):
        images.append(link_img['href'])
    if images:
        fragment['images'] = images

    # Heading fallbacks for the product title.
    h1 = _run_step('dom.h1', lambda: soup.find('h1'), url=final_url, log=logger)
    if h1 is not None:
        text = clean_text(h1.get_text())
        if text:
            fragment['h1'] = text

    # Store currency sometimes appears in embedded scripts - probe it (never guess USD).
    currency_probe = _run_step(
        'dom.currency_probe',
        lambda: next(
            (
                match.group(1)
                for script in soup.find_all('script')
                if isinstance(script.string, str)
                for match in [re.search(r'currency[\'"]?\s*[:=]\s*[\'"]([A-Z]{3})[\'"]', script.string)]
                if match
            ),
            None,
        ),
        url=final_url, log=logger,
    )
    if looks_like_currency(currency_probe):
        fragment.setdefault('script_currency', currency_probe)

    # DOM image fallback: only real Shopify CDN product images, used when all
    # structured sources produced no images. No URLs are invented.
    if 'images' not in fragment:
        dom_images: list[str] = []
        for img in _run_step(
            'dom.img_fallback', lambda: soup.find_all('img', src=True), url=final_url, log=logger,
        ) or []:
            src = img.get('src') or ''
            if not isinstance(src, str):
                continue
            lowered = src.lower()
            if ('cdn.shopify.com' in lowered or '/cdn/shop/' in lowered or '/files/' in lowered) \
                    and 'logo' not in lowered and 'icon' not in lowered and 'sprite' not in lowered:
                dom_images.append(src)
            if len(dom_images) >= MAX_DOM_FALLBACK_IMAGES:
                break
        if dom_images:
            fragment['dom_fallback_images'] = dom_images
    return fragment


# --------------------------------------------------------------------------------------
# Description cleaning and Markdown generation
# --------------------------------------------------------------------------------------

_DESCRIPTION_STRIP_TAGS = ('script', 'style', 'noscript', 'nav', 'header', 'footer', 'aside',
                           'form', 'iframe', 'button', 'svg', 'template')
_BOILERPLATE_ID_RE = re.compile(r'(cookie|gdpr|newsletter|popup|modal|subscribe|banner-ad|social)', re.IGNORECASE)


def clean_html_fragment(fragment_html: str) -> str:
    """Strip scripts/styles/nav/footer/cookie banners/hidden boilerplate from description HTML."""
    if not fragment_html or not fragment_html.strip():
        return ''
    soup = BeautifulSoup(fragment_html, 'html.parser')
    for tag in soup.find_all(_DESCRIPTION_STRIP_TAGS):
        tag.decompose()
    for element in soup.find_all(id=_BOILERPLATE_ID_RE):
        element.decompose()
    for element in soup.find_all(class_=_BOILERPLATE_ID_RE):
        element.decompose()
    from bs4 import Comment
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()
    return str(soup)


def html_to_markdown(fragment_html: str) -> str:
    """Convert a cleaned HTML fragment to readable Markdown (markdownify)."""
    if not fragment_html:
        return ''
    try:
        markdown = MarkdownConverter(heading_style='ATX', bullets='-', strip=['img']).convert(fragment_html)
    except Exception as exc:
        log_step_error('html_to_markdown', exc)
        return ''
    return re.sub(r'\n{3,}', '\n\n', markdown.strip())


def build_product_markdown(record: dict[str, Any]) -> str:
    """Build the product Markdown from normalized structured fields (never the whole page)."""
    lines: list[str] = [f"# {record['title']}", '']
    if record.get('vendor'):
        lines.append(f"**Brand:** {record['vendor']}")
    if record.get('product_type'):
        lines.append(f"**Product type:** {record['product_type']}")
    price_min, price_max = record.get('price_min'), record.get('price_max')
    if price_min is not None:
        currency = record.get('currency')
        prefix = f'{currency} ' if currency else ''
        if price_max is not None and price_min != price_max:
            lines.append(f'**Price:** {prefix}{price_min}–{price_max}')
        else:
            lines.append(f'**Price:** {prefix}{price_min}')
    if record.get('available') is True:
        lines.append('**Availability:** In stock')
    elif record.get('available') is False:
        lines.append('**Availability:** Out of stock')
    if len(lines) > 2:
        lines.append('')

    description_md = record.get('_description_markdown')
    if description_md:
        lines.append('## Description')
        lines.append('')
        lines.append(description_md)
        lines.append('')

    options = record.get('options') or []
    variants = record.get('variants') or []
    if options or variants:
        lines.append('## Options and variants')
        lines.append('')
        for option in options:
            if option.get('values'):
                lines.append(f"**{option['name']}:** {', '.join(option['values'])}")
        for variant in variants:
            label = variant.get('title') or ' / '.join(str(v) for v in variant.get('options', {}).values()) or 'Variant'
            bits: list[str] = []
            price = variant.get('price')
            if price is not None:
                currency = record.get('currency')
                price_text = f'{currency} {price}' if currency else str(price)
                compare = variant.get('compare_at_price')
                if compare:
                    price_text += f' ~~{currency + " " if currency else ""}{compare}~~'
                bits.append(price_text)
            if variant.get('sku'):
                bits.append(f"SKU: {variant['sku']}")
            if variant.get('available') is True:
                bits.append('In stock')
            elif variant.get('available') is False:
                bits.append('Out of stock')
            lines.append(f'- {label} — {", ".join(bits)}' if bits else f'- {label}')
        lines.append('')

    images = record.get('images') or []
    if images:
        lines.append('## Images')
        lines.append('')
        lines.extend(f'- {image}' for image in images)
        lines.append('')

    if record.get('canonical_url'):
        lines.append('## Product URL')
        lines.append('')
        lines.append(record['canonical_url'])
        lines.append('')
    return '\n'.join(lines).strip()


# --------------------------------------------------------------------------------------
# Record assembly (merge tiers -> output contract)
# --------------------------------------------------------------------------------------

TIER_ORDER = ('json_ld', 'shopify_meta', 'html_metadata')


def _resolve_images(candidates: Iterable[str], final_url: str) -> list[str]:
    """Resolve, dedupe (by scheme+host+path, keeping order) and filter image URLs."""
    resolved: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        absolute = normalize_url(candidate, base=final_url)
        if not absolute:
            continue
        parts = urlsplit(absolute)
        dedupe_key = f'{parts.scheme}://{parts.netloc}{parts.path}'
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        resolved.append(absolute)
    return resolved


def build_product_record(
    frag_ld: dict[str, Any],
    frag_meta: dict[str, Any],
    frag_dom: dict[str, Any],
    *,
    request_url: str,
    final_url: str,
    store_domain: str,
    url_handle: str | None,
    include_variants: bool,
    include_markdown: bool,
    html: str,
    log: logging.Logger | None = None,
) -> dict[str, Any] | None:
    """Merge the tier fragments into the stable output contract record, or None."""
    logger = log or Actor.log
    fragments = {'json_ld': frag_ld, 'shopify_meta': frag_meta, 'html_metadata': frag_dom}
    sources: set[str] = set()

    def pick(key: str) -> tuple[Any, str | None]:
        for tier in TIER_ORDER:
            value = fragments[tier].get(key)
            if value is not None and value != '' and value != []:
                sources.add(tier)
                return value, tier
        return None, None

    title = pick('title')[0] or pick('og_title')[0] or pick('h1')[0]
    if not title:
        return None

    vendor = pick('vendor')[0]
    product_type = pick('product_type')[0]
    handle = url_handle or frag_meta.get('handle')
    product_id = pick('product_id')[0]
    canonical_url = pick('canonical')[0] or frag_dom.get('og_url') or normalize_url(final_url)
    product_url = normalize_url(request_url) or canonical_url
    if not canonical_url:
        return None

    # Currency: LD priceCurrency > meta currency > DOM meta > script probe. Never guessed.
    currency = None
    for probe in (frag_ld.get('currency'), frag_meta.get('currency'), frag_dom.get('currency'), frag_dom.get('script_currency')):
        if looks_like_currency(probe):
            currency = probe.strip()
            sources.add('json_ld' if probe is frag_ld.get('currency') else 'shopify_meta' if probe is frag_meta.get('currency') else 'html_metadata')
            break

    # ---- Prices (Decimal only; missing stays None, never zero) ----
    price_candidates: list[Decimal] = []
    compare_candidates: list[Decimal] = []
    ld_offers: dict[str, Any] = frag_ld.get('ld_offers') or {}
    for value in (ld_offers.get('low'), ld_offers.get('high'), *ld_offers.get('prices', []), frag_dom.get('price')):
        if value is not None:
            price_candidates.append(value)
    bounds = frag_meta.get('product_price_bounds')
    if bounds:
        price_candidates.extend(v for v in bounds if v is not None)

    variants: list[dict[str, Any]] = []
    if include_variants and frag_meta.get('variants'):
        for variant in frag_meta['variants']:
            price = variant['price']
            if price is not None:
                price_candidates.append(price)
            if variant['compare_at_price'] is not None:
                compare_candidates.append(variant['compare_at_price'])
            variants.append({
                'variant_id': variant['variant_id'],
                'title': variant['title'],
                'sku': variant['sku'],
                'barcode': variant['barcode'],
                'available': variant['available'],
                'price': money_str(variant['price']),
                'compare_at_price': money_str(variant['compare_at_price']),
                'options': variant['options'],
            })
        if frag_meta.get('variants'):
            sources.add('shopify_meta')
    options = frag_meta.get('options') or []
    if options:
        sources.add('shopify_meta')

    if price_candidates:
        price_min = money_str(min(price_candidates))
        price_max = money_str(max(price_candidates))
        sources.add('json_ld' if ld_offers else 'shopify_meta' if bounds else 'html_metadata')
    else:
        price_min = price_max = None
    if compare_candidates:
        compare_min = money_str(min(compare_candidates))
        compare_max = money_str(max(compare_candidates))
    else:
        compare_min = compare_max = None

    available = frag_meta.get('available')
    if available is None:
        available = frag_ld.get('available')
    if available is None and variants:
        variant_availability = [v['available'] for v in variants if v['available'] is not None]
        if variant_availability:
            available = any(variant_availability)
            sources.add('shopify_meta')

    # ---- Description (cleaned; Markdown built from it) ----
    description_text = None
    description_markdown = None
    description_html = frag_ld.get('description_html') or frag_meta.get('description_html')
    if description_html:
        cleaned = clean_html_fragment(description_html)
        soup_text = _run_step('description.get_text', lambda: BeautifulSoup(cleaned, 'html.parser').get_text(' ', strip=True),
                              url=final_url, html=html, log=logger) or ''
        description_text = clean_text(soup_text) or None
        description_markdown = html_to_markdown(cleaned) or None
        sources.add('json_ld' if description_html is frag_ld.get('description_html') else 'shopify_meta')
    if description_text is None and frag_dom.get('og_description'):
        description_text = frag_dom['og_description']
        description_markdown = frag_dom['og_description']
        sources.add('html_metadata')

    # ---- Images ----
    image_candidates = (
        list(frag_ld.get('images') or [])
        + list(frag_meta.get('images') or [])
        + list(frag_dom.get('images') or [])
        + list(frag_dom.get('dom_fallback_images') or [])
    )
    images = _resolve_images(image_candidates, final_url)

    tags = pick('tags')[0] or []
    published_at = pick('published_at')[0]

    if not handle:
        slug_match = PRODUCT_PATH_RE.search(urlsplit(canonical_url).path or '')
        handle = slug_match.group(1) if slug_match else None
    if handle:
        product_key = f'{store_domain}:{handle}'
    else:
        digest = hashlib.sha1(canonical_url.encode('utf-8')).hexdigest()[:16]
        product_key = f'{store_domain}:url:{digest}'

    record: dict[str, Any] = {
        'schema_version': SCHEMA_VERSION,
        'record_type': RECORD_TYPE,
        'product_key': product_key,
        'product_id': product_id,
        'store_domain': store_domain,
        'product_url': product_url,
        'canonical_url': canonical_url,
        'extracted_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'title': title,
        'handle': handle,
        'vendor': vendor,
        'product_type': product_type,
        'description_text': description_text,
        'currency': currency,
        'price_min': price_min,
        'price_max': price_max,
        'compare_at_price_min': compare_min,
        'compare_at_price_max': compare_max,
        'available': available,
        'options': options,
        'variants': variants,
        'images': images,
        'tags': tags,
        'published_at': published_at,
        'source_fields': sorted(sources),
    }
    if include_markdown:
        record['_description_markdown'] = description_markdown
        record['markdown'] = build_product_markdown(record)
        record.pop('_description_markdown', None)
    return record


def extract_product_from_html(
    html: str,
    *,
    request_url: str,
    final_url: str,
    store_domain: str,
    include_variants: bool,
    include_markdown: bool,
    log: logging.Logger | None = None,
) -> dict[str, Any] | None:
    """Parse one rendered product page through the extraction tiers and build the record."""
    logger = log or Actor.log
    soup = _run_step('bs4.parse', lambda: BeautifulSoup(html, 'html.parser'), url=final_url, html=html, log=logger)
    if soup is None:
        return None
    if not looks_like_shopify(html):
        logger.warning(f'[NOT_SHOPIFY] product page does not look like a Shopify storefront | url={final_url}')
        return None

    frag_dom = _run_step('dom.extract', lambda: extract_dom_fields(soup, final_url=final_url, log=logger),
                         url=final_url, html=html, default={}, log=logger) or {}
    ld_products = _run_step('json_ld.collect', lambda: collect_ld_products(soup, url=final_url, log=logger),
                            url=final_url, html=html, default=[], log=logger) or []
    ld_main = _run_step('json_ld.pick_main', lambda: pick_main_ld_product(ld_products, final_url=final_url,
                                                                        og_title=frag_dom.get('og_title')),
                        url=final_url, html=html, log=logger)
    frag_ld = _run_step('json_ld.extract_fields', lambda: extract_ld_product_fields(ld_main),
                        url=final_url, html=html, default={}, log=logger) if ld_main else {}
    meta_json = _run_step('shopify_meta.find', lambda: find_shopify_product_json(soup, url=final_url, log=logger),
                          url=final_url, html=html, log=logger)
    frag_meta = _run_step('shopify_meta.extract_fields', lambda: extract_shopify_meta_fields(meta_json),
                          url=final_url, html=html, default={}, log=logger) if meta_json else {}

    url_handle = None
    path_match = PRODUCT_PATH_RE.search(urlsplit(final_url or request_url).path or '')
    if path_match:
        url_handle = path_match.group(1)

    record = _run_step(
        'record.build',
        lambda: build_product_record(
            frag_ld, frag_meta, frag_dom,
            request_url=request_url, final_url=final_url, store_domain=store_domain,
            url_handle=url_handle, include_variants=include_variants,
            include_markdown=include_markdown, html=html, log=logger,
        ),
        url=final_url, html=html, log=logger,
    )
    if record is None:
        log_selector_miss(
            'product.identity', url=final_url,
            selector='json_ld Product.name / meta og:title / h1 / shopify meta product title',
            extra='could not establish product title', html=html, log=logger,
        )
    return record


# --------------------------------------------------------------------------------------
# robots.txt / sitemap / pagination parsing
# --------------------------------------------------------------------------------------


def parse_sitemap_urls_from_robots(robots_text: str, base_url: str) -> list[str]:
    """Extract Sitemap: URLs from a robots.txt body (deduplicated, order preserved)."""
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r'(?im)^\s*sitemap\s*:\s*(\S+)', robots_text or ''):
        url = normalize_url(html_lib.unescape(match.group(1)), base=base_url)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def parse_sitemap_content(content: str) -> tuple[bool, list[str]]:
    """Parse sitemap XML (index or urlset). Returns (is_sitemap_index, locations)."""
    if not content:
        return False, []
    locations: list[str] = []
    seen: set[str] = set()
    for match in re.findall(r'<loc>\s*(.*?)\s*</loc>', content, re.IGNORECASE | re.DOTALL):
        url = html_lib.unescape(match).strip()
        if url and url not in seen:
            seen.add(url)
            locations.append(url)
    if not locations:
        # BeautifulSoup fallback in case the browser wrapped/serialized the XML oddly.
        def _bs4_locs() -> list[str]:
            soup = BeautifulSoup(content, 'html.parser')
            return [tag.get_text(strip=True) for tag in soup.find_all('loc')]
        for url in _bs4_locs() or []:
            if url and url not in seen:
                seen.add(url)
                locations.append(url)
    is_index = bool(re.search(r'<sitemapindex', content, re.IGNORECASE))
    return is_index, locations


def find_next_collection_url(soup: BeautifulSoup, page_url: str, final_url: str) -> str | None:
    """Find an explicit next-page link: rel=next, link[rel=next], or common pagination text."""
    resolved_base = final_url or page_url

    def _candidate(anchor: Any) -> str | None:
        href = anchor.get('href') if anchor else None
        if not href or not str(href).strip():
            return None
        return normalize_url(str(href), base=resolved_base)

    try:
        for anchor in soup.find_all('a', rel=lambda v: v and any(str(r).lower() == 'next' for r in v)):
            candidate = _candidate(anchor)
            if candidate:
                return candidate
    except Exception:
        pass
    try:
        for link in soup.find_all('link', rel=lambda v: v and any(str(r).lower() == 'next' for r in v)):
            candidate = _candidate(link)
            if candidate:
                return candidate
    except Exception:
        pass
    try:
        for anchor in soup.find_all('a', href=True):
            class_id = ' '.join(anchor.get('class', [])) + ' ' + str(anchor.get('id', ''))
            text = anchor.get_text(strip=True)
            disabled = 'disabled' in class_id or anchor.get('aria-disabled') in ('true', 'True')
            if disabled:
                continue
            if re.search(r'(next|older)', class_id, re.IGNORECASE) or NEXT_PAGE_TEXT_RE.match(text or ''):
                candidate = _candidate(anchor)
                if candidate and 'page=' in candidate:
                    return candidate
    except Exception:
        pass
    return None


def next_page_fallback_url(page_url: str) -> str | None:
    """Conservative ?page=N+1 fallback (only used when the current page yielded new products)."""
    parts = urlsplit(page_url)
    query = parse_qsl(parts.query, keep_blank_values=True)
    if any(key == 'page' and value.isdigit() for key, value in query):
        next_query = [(key, str(int(value) + 1) if key == 'page' and value.isdigit() else value) for key, value in query]
    else:
        next_query = query + [('page', '2')]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(next_query), ''))


# --------------------------------------------------------------------------------------
# Crawl runtime: request handler, budget enforcement, publishing, error handlers
# --------------------------------------------------------------------------------------


class CrawlRuntime:
    """Holds the crawl state and implements the per-page-type handlers.

    Extraction lives in the pure helpers above; this class only orchestrates
    navigation results, budgets, deduplication, and dataset publishing.
    """

    def __init__(
        self,
        config: CrawlConfig,
        state: CrawlState,
        crawler: PlaywrightCrawler | None,
        *,
        pay_per_event: bool,
        legacy_max_paid_items: int | None,
        legacy_initial_items: int,
    ) -> None:
        self.config = config
        self.state = state
        self.crawler = crawler  # assigned for real in main() right after PlaywrightCrawler(...)
        self.pay_per_event = pay_per_event
        self.legacy_max_paid_items = legacy_max_paid_items
        self.legacy_initial_items = legacy_initial_items
        self.stores_ok: set[str] = set()
        self._adopted_hosts: set[str] = set()
        self._restricted_log_once: set[str] = set()

    # ---------------- budget bookkeeping ----------------

    def _log_limit_stop(self, message: str) -> None:
        if not self.state.limit_stop_logged:
            Actor.log.warning(f'[LIMIT_STOP] {message} - no new requests will be enqueued')
            self.state.limit_stop_logged = True
        else:
            Actor.log.debug(f'[LIMIT_STOP] {message}')

    def _charge_page(self, store_domain: str) -> bool:
        """Count one processed page against the per-store budget. False when over budget."""
        state = self.state
        if state.pages_used[store_domain] >= self.config.max_pages_per_store:
            self._log_limit_stop(f'page budget reached for {store_domain}')
            return False
        state.pages_used[store_domain] += 1
        if state.pages_pending[store_domain] > 0:
            state.pages_pending[store_domain] -= 1
        return True

    def _store_pages_remaining(self, store_domain: str) -> int:
        state = self.state
        return max(
            0,
            self.config.max_pages_per_store
            - state.pages_used[store_domain]
            - state.pages_pending[store_domain],
        )

    # ---------------- enqueueing ----------------

    async def _enqueue(
        self,
        context: Any,
        url: str,
        *,
        label: str,
        store_domain: str,
        user_data: dict[str, Any] | None = None,
    ) -> bool:
        """Canonicalize, guard (visited/budget/blocked/host) and enqueue one request."""
        state = self.state
        if state.charge_limit_hit or state.products_limit_hit:
            return False
        if state.is_restricted(store_domain):
            return False
        normalized = normalize_url(url)
        if not normalized or normalized in state.visited_urls:
            return False
        if _store_pages_remaining_guard(self, store_domain):
            self._log_limit_stop(f'page budget exhausted for {store_domain}')
            return False

        host = urlsplit(normalized).hostname or ''
        if host not in self.config.allowed_hosts and host not in self._adopted_hosts:
            Actor.log.debug(f'[ENQUEUE_SKIP] external-domain link not followed | url={normalized}')
            return False

        path = urlsplit(normalized).path or '/'
        if label in ('product', 'collection'):
            if path_is_blocked(path):
                Actor.log.debug(f'[ENQUEUE_SKIP] blocked path | url={normalized}')
                return False
            if ASSET_EXT_RE.search(path):
                return False
        if label == 'product':
            pending = state.products_pending[store_domain]
            if state.published + pending >= self.config.max_products:
                self._log_limit_stop(
                    f'product enqueue cap reached for {store_domain} '
                    f'(published={state.published}, pending={pending}, max={self.config.max_products})'
                )
                return False
            state.products_pending[store_domain] += 1

        state.visited_urls.add(normalized)
        state.pages_pending[store_domain] += 1
        payload = {'page_type': label, 'store_domain': store_domain}
        if user_data:
            payload.update(user_data)
        request = Request.from_url(normalized, label=label, user_data=payload)
        try:
            await context.add_requests([request])
        except Exception as exc:
            # Roll back bookkeeping so a queue hiccup cannot leak budget counts.
            state.visited_urls.discard(normalized)
            state.pages_pending[store_domain] -= 1
            if label == 'product':
                state.products_pending[store_domain] -= 1
            log_step_error('enqueue.add_requests', exc, url=normalized, tag='NAV_ERROR')
            return False
        return True

    # ---------------- request handler ----------------

    async def handle_request(self, context: PlaywrightCrawlingContext) -> None:
        request = context.request
        url = request.url
        label = request.label or request.user_data.get('page_type') or 'homepage'
        store_domain = request.user_data.get('store_domain') or ''
        final_url = request.loaded_url or url

        state = self.state
        if state.charge_limit_hit or state.products_limit_hit:
            self._log_limit_stop('crawl stopping due to product/charging limit')
            return
        if not store_domain:
            store_domain = normalize_domain(urlsplit(final_url).hostname) or 'unknown'
        if state.is_restricted(store_domain):
            if store_domain not in self._restricted_log_once:
                self._restricted_log_once.add(store_domain)
                Actor.log.warning(
                    f'[ACCESS_DENIED] skipping request for restricted domain '
                    f'{store_domain} (reason={state.restricted_domains[store_domain]!r}) | url={url}'
                )
            return

        # Redirect safety: if the navigation landed on a different host, verify it is
        # still a public target before processing (SSRF protection for redirects).
        try:
            final_host = (urlsplit(final_url).hostname or '').lower()
        except ValueError:
            final_host = ''
        if final_host:
            request_host = (urlsplit(url).hostname or '').lower()
            if normalize_domain(final_host) != normalize_domain(request_host) \
                    and final_host not in self.config.allowed_hosts \
                    and final_host not in self._adopted_hosts:
                ok, reason = hostname_is_public(final_host, resolve=True)
                if not ok:
                    state.restrict(store_domain, f'redirect to non-public host {final_host}')
                    Actor.log.error(f'[SSRF] redirect to non-public destination rejected ({reason}) | url={url} -> {final_url}')
                    return
                self._adopted_hosts.add(final_host)
                Actor.log.info(f'[REDIRECT] adopting redirect host {final_host} for this run | url={url} -> {final_url}')

        if not self._charge_page(store_domain):
            return
        if label == 'product' and state.products_pending[store_domain] > 0:
            state.products_pending[store_domain] -= 1

        try:
            html = await context.page.content()
        except Exception as exc:
            state.nav_errors += 1
            log_step_error('page.content', exc, url=url, tag='NAV_ERROR')
            raise  # let Crawlee treat it as a navigation failure (bounded retries)

        # JS-hydrated storefronts often render product anchors only after networking
        # settles; give them a short, bounded grace period (never required to succeed).
        if label in ('homepage', 'collection'):
            try:
                await context.page.wait_for_load_state('networkidle', timeout=8000)
                html = await context.page.content()
            except Exception:
                pass  # bounded wait; proceed with whatever rendered before it

        if looks_like_captcha(html):
            state.restrict(store_domain, 'captcha_or_bot_challenge')
            Actor.log.error(
                f'[ACCESS_DENIED] bot challenge / CAPTCHA detected - domain {store_domain} will not be '
                f'retried through proxies or identity rotation | url={final_url}'
            )
            return

        handlers = {
            'homepage': self._handle_homepage,
            'robots': self._handle_robots,
            'sitemap': self._handle_sitemap,
            'collection': self._handle_collection,
            'product': self._handle_product,
        }
        handler = handlers.get(label, self._handle_homepage)
        try:
            await handler(context, html=html, store_domain=store_domain, url=url, final_url=final_url)
        except Exception as exc:
            state.parse_errors += 1
            log_step_error(f'handler.{label}', exc, url=url, html=html, tag='HANDLER_ERROR')

    # ---------------- page-type handlers ----------------

    async def _handle_homepage(self, context: Any, *, html: str, store_domain: str, url: str, final_url: str) -> None:
        if not looks_like_shopify(html):
            Actor.log.warning(
                f'[NOT_SHOPIFY] start URL does not look like a Shopify storefront - '
                f'it will not be treated as an extracted store | url={final_url}'
            )
            return
        Actor.log.info(f'[STORE] homepage OK, starting discovery | url={final_url}')
        final_parts = urlsplit(final_url)
        robots_url = urlunsplit((final_parts.scheme, final_parts.netloc, '/robots.txt', '', ''))
        await self._enqueue(context, robots_url, label='robots', store_domain=store_domain)

        # Secondary discovery: featured product/collection links on the homepage.
        # Caps are budget-aware so sitemap discovery (the primary source) keeps room.
        budget = self.config.max_pages_per_store
        max_products_here = max(1, min(MAX_HOMEPAGE_DIRECT_PRODUCTS, budget // 4))
        max_collections_here = max(1, min(MAX_HOMEPAGE_DIRECT_COLLECTIONS, budget // 8))
        soup = _run_step('homepage.soup', lambda: BeautifulSoup(html, 'html.parser'), url=final_url, html=html)
        if soup is None:
            return
        products = 0
        collections = 0
        seen_here: set[str] = set()
        for anchor in _run_step(
            'homepage.links', lambda: soup.find_all('a', href=True), url=final_url, html=html, default=[],
        ) or []:
            href = urljoin(final_url, anchor.get('href', ''))
            normalized = normalize_url(href)
            if not normalized or normalized in seen_here:
                continue
            seen_here.add(normalized)
            kind, _ = classify_url_path(urlsplit(normalized).path or '')
            if kind == 'product' and products < max_products_here:
                if await self._enqueue(context, normalized, label='product', store_domain=store_domain):
                    products += 1
            elif kind == 'collection' and collections < max_collections_here:
                if await self._enqueue(context, normalized, label='collection', store_domain=store_domain):
                    collections += 1
            if products >= max_products_here and collections >= max_collections_here:
                break
        Actor.log.info(f'[DISCOVER] homepage links | url={final_url} products={products} collections={collections}')

    async def _handle_robots(self, context: Any, *, html: str, store_domain: str, url: str, final_url: str) -> None:
        text = _run_step('robots.text', lambda: BeautifulSoup(html, 'html.parser').get_text(), url=final_url, html=html) or ''
        sitemap_urls = _run_step(
            'robots.sitemaps', lambda: parse_sitemap_urls_from_robots(text, final_url), url=final_url, html=html, default=[],
        )
        enqueued = 0
        skipped = 0
        for sitemap_url in sitemap_urls[:MAX_SITEMAP_FILES_PER_STORE]:
            if urlsplit(sitemap_url).hostname not in self.config.allowed_hosts:
                skipped += 1
                Actor.log.debug(f'[SITEMAP] ignoring external sitemap host | url={sitemap_url}')
                continue
            if await self._enqueue(context, sitemap_url, label='sitemap', store_domain=store_domain, user_data={'depth': 1}):
                enqueued += 1
        if enqueued:
            Actor.log.info(
                f'[SITEMAP] {enqueued} sitemap URL(s) enqueued from robots.txt '
                f'(found={len(sitemap_urls)}, host_skipped={skipped}) | store={store_domain}'
            )
        elif sitemap_urls:
            Actor.log.warning(
                f'[SITEMAP] robots.txt advertised {len(sitemap_urls)} sitemap URL(s) but none could be '
                f'enqueued (page budget or host restrictions) | store={store_domain}'
            )
        else:
            final_parts = urlsplit(final_url)
            fallback = urlunsplit((final_parts.scheme, final_parts.netloc, '/sitemap.xml', '', ''))
            Actor.log.info(f'[SITEMAP] no usable sitemap in robots.txt, trying {fallback} | store={store_domain}')
            await self._enqueue(context, fallback, label='sitemap', store_domain=store_domain, user_data={'depth': 1})

    async def _handle_sitemap(self, context: Any, *, html: str, store_domain: str, url: str, final_url: str) -> None:
        state = self.state
        depth = int(context.request.user_data.get('depth', 1) or 1)
        is_index, locations = _run_step(
            'sitemap.parse', lambda: parse_sitemap_content(html), url=final_url, html=html, default=(False, []),
        )
        if not locations:
            Actor.log.warning(f'[SITEMAP] unparsable sitemap page | url={final_url} | html_snippet={_html_snippet(html, 300)}')
            return
        if is_index:
            if depth >= MAX_SITEMAP_DEPTH:
                self._log_limit_stop(f'sitemap nesting depth cap reached for {store_domain}')
                return
            remaining_slots = MAX_SITEMAP_FILES_PER_STORE - state.sitemap_files_used[store_domain]
            if remaining_slots <= 0:
                self._log_limit_stop(f'sitemap file cap reached for {store_domain}')
                return
            enqueued = 0
            for child in locations[:remaining_slots]:
                if urlsplit(child).hostname not in self.config.allowed_hosts:
                    continue
                if await self._enqueue(context, child, label='sitemap', store_domain=store_domain, user_data={'depth': depth + 1}):
                    enqueued += 1
                    state.sitemap_files_used[store_domain] += 1
            Actor.log.info(f'[SITEMAP] index expanded | url={final_url} children_enqueued={enqueued}')
            return

        products = 0
        collections = 0
        product_slots = min(self._store_pages_remaining(store_domain), MAX_SITEMAP_PRODUCTS_PER_FILE)
        for location in locations:
            normalized = normalize_url(location)
            if not normalized:
                continue
            host = urlsplit(normalized).hostname or ''
            if host not in self.config.allowed_hosts and host not in self._adopted_hosts:
                continue
            kind, _ = classify_url_path(urlsplit(normalized).path or '')
            if kind == 'product' and products < product_slots:
                if await self._enqueue(context, normalized, label='product', store_domain=store_domain):
                    products += 1
            elif kind == 'collection' and collections < MAX_COLLECTIONS_FROM_SITEMAP:
                if await self._enqueue(context, normalized, label='collection', store_domain=store_domain):
                    collections += 1
            if products >= product_slots and collections >= MAX_COLLECTIONS_FROM_SITEMAP:
                break
        Actor.log.info(f'[SITEMAP] urlset processed | url={final_url} products_enqueued={products} collections_enqueued={collections}')

    async def _handle_collection(self, context: Any, *, html: str, store_domain: str, url: str, final_url: str) -> None:
        if not looks_like_shopify(html):
            Actor.log.warning(f'[NOT_SHOPIFY] collection page is not a Shopify storefront | url={final_url}')
            return
        soup = _run_step('collection.soup', lambda: BeautifulSoup(html, 'html.parser'), url=final_url, html=html)
        if soup is None:
            return
        enqueued_products = 0
        seen_here: set[str] = set()
        for anchor in _run_step(
            'collection.links', lambda: soup.find_all('a', href=True), url=final_url, html=html, default=[],
        ) or []:
            href = urljoin(final_url, anchor.get('href', ''))
            normalized = normalize_url(href)
            if not normalized or normalized in seen_here:
                continue
            seen_here.add(normalized)
            kind, _ = classify_url_path(urlsplit(normalized).path or '')
            if kind != 'product':
                continue
            if await self._enqueue(context, normalized, label='product', store_domain=store_domain):
                enqueued_products += 1
        Actor.log.info(f'[COLLECTION] products from listing | url={final_url} enqueued={enqueued_products}')

        # --- Pagination (explicit next link preferred; conservative page-number fallback) ---
        if enqueued_products == 0:
            Actor.log.info(f'[PAGINATION_STOP] page produced no new products | url={final_url}')
            return
        next_url = _run_step(
            'collection.next_link', lambda: find_next_collection_url(soup, url, final_url),
            url=final_url, html=html, default=None,
        )
        if next_url is None:
            next_url = _run_step(
                'collection.next_fallback', lambda: next_page_fallback_url(final_url or url),
                url=final_url, html=html, default=None,
            )
            if next_url:
                Actor.log.info(f'[PAGINATION] no explicit next link; trying page-number fallback {next_url}')
        if not next_url:
            Actor.log.info(f'[PAGINATION_STOP] no usable next page | url={final_url}')
            return
        if next_url in self.state.visited_urls:
            Actor.log.info(f'[PAGINATION_STOP] next page already visited (loop guard) | url={next_url}')
            return
        await self._enqueue(context, next_url, label='collection', store_domain=store_domain)

    async def _handle_product(self, context: Any, *, html: str, store_domain: str, url: str, final_url: str) -> None:
        state = self.state
        if state.published >= self.config.max_products:
            self._log_limit_stop('global product limit already reached')
            return
        state.product_pages_processed += 1
        record = extract_product_from_html(
            html,
            request_url=url,
            final_url=final_url,
            store_domain=store_domain,
            include_variants=self.config.include_variants,
            include_markdown=self.config.include_markdown,
        )
        if record is None:
            state.parse_errors += 1
            return
        if record['product_key'] in state.published_keys:
            state.duplicates += 1
            Actor.log.info(f"[DUPLICATE] product already published | key={record['product_key']} | url={final_url}")
            return
        await self.publish_product(record)

    # ---------------- publishing and monetization ----------------

    async def publish_product(self, record: dict[str, Any]) -> bool:
        """Publish exactly one dataset item for one unique product (spec order enforced)."""
        state = self.state
        # 1. Validate: a real product needs a title and a stable identity; partial is OK.
        if not record.get('title') or not record.get('canonical_url'):
            Actor.log.error(f"[INVALID_PRODUCT] refusing to publish a product without title/canonical URL | url={record.get('canonical_url') or record.get('product_url')}")
            return False
        async with state.publish_lock:
            # 2. Re-check product key uniqueness and limits atomically.
            key = record['product_key']
            if key in state.published_keys:
                state.duplicates += 1
                Actor.log.info(f'[DUPLICATE] product already published | key={key}')
                return False
            if state.published >= self.config.max_products:
                state.products_limit_hit = True
                self._log_limit_stop(f'global product limit reached ({self.config.max_products})')
                self.crawler.stop('Global product limit reached.')
                return False
            # 3. Apply the active pricing model and push exactly one item.
            try:
                if self.pay_per_event:
                    charge = await Actor.push_data(record, charged_event_name=CHARGED_EVENT_NAME)
                else:
                    if self.legacy_max_paid_items is not None and self.legacy_max_paid_items > 0:
                        if self.legacy_initial_items + state.published >= self.legacy_max_paid_items:
                            state.charge_limit_hit = True
                            Actor.log.warning(
                                f'[CHARGE_LIMIT] legacy pay-per-result cap reached '
                                f'(initial={self.legacy_initial_items}, published={state.published}, '
                                f'max={self.legacy_max_paid_items}) - stopping the crawl'
                            )
                            self.crawler.stop('Legacy pay-per-result charging limit reached.')
                            return False
                    charge = await Actor.push_data(record)
            except Exception as exc:
                state.parse_errors += 1
                log_step_error('publish.push_data', exc, url=record.get('product_url'), tag='PUBLISH_ERROR')
                return False
            # 4. Counters update only after a successful push (no double-charging).
            state.published += 1
            state.published_keys.add(key)
            if self.pay_per_event and getattr(charge, 'event_charge_limit_reached', False):
                state.charge_limit_hit = True
                Actor.log.warning(
                    f'[CHARGE_LIMIT] event charge limit reached after {state.published} product(s) '
                    f'- stopping scheduling and shutting down gracefully'
                )
                self.crawler.stop('Event charge limit reached.')
            if state.published >= self.config.max_products:
                state.products_limit_hit = True
                self._log_limit_stop(f'global product limit reached ({self.config.max_products})')
                self.crawler.stop('Global product limit reached.')
            Actor.log.info(f"[PRODUCT_OK] published | key={key} | total={state.published}/{self.config.max_products}")
            if record.get('store_domain'):
                self.stores_ok.add(record['store_domain'])
            return True

    # ---------------- crawler-level callbacks ----------------

    async def on_error(self, context: Any, error: Exception) -> None:
        """Called for each failed attempt before a bounded retry."""
        state = self.state
        state.nav_errors += 1
        request = context.request
        retry_note = ''
        if isinstance(error, RequestThrottledError):
            retry_note = ' | rate_limited=request deferred to domain cooldown (Retry-After honored by ThrottlingRequestManager)'
        elif isinstance(error, ProxyError):
            retry_note = ' | transport-level proxy failure (normal rotation)'
        elif isinstance(error, (HttpStatusCodeError, HttpClientStatusCodeError)):
            retry_note = f' | http_status={getattr(error, "status_code", "?")}'
        Actor.log.warning(
            f'[NAV_RETRY] navigation/processing error (attempt {request.retry_count + 1}) '
            f'| url={request.url}{retry_note} | error={type(error).__name__}'
        )

    async def on_failed(self, context: Any, error: Exception) -> None:
        """Called once a request has exhausted retries (or failed permanently)."""
        state = self.state
        request = context.request
        url = request.url
        label = request.label or request.user_data.get('page_type') or 'homepage'
        store_domain = request.user_data.get('store_domain') or ''
        status_code = getattr(error, 'status_code', None)

        if isinstance(error, HttpClientStatusCodeError) and status_code == 403:
            if store_domain:
                state.restrict(store_domain, 'http_403_forbidden')
            Actor.log.error(
                f'[ACCESS_DENIED] HTTP 403 - domain will not be retried or rotated through proxies | url={url}'
            )
            return
        if isinstance(error, HttpClientStatusCodeError) and status_code == 429:
            if store_domain:
                state.restrict(store_domain, 'http_429_rate_limit')
            Actor.log.error(f'[RATE_LIMIT] HTTP 429 exhausted its cooldown budget - domain paused for this run | url={url}')
            return
        if isinstance(error, HttpClientStatusCodeError) and status_code == 404:
            state.not_found += 1
            Actor.log.info(f'[NOT_FOUND] page does not exist (no retries) | url={url}')
        else:
            state.nav_errors += 1
            Actor.log.error(
                f'[NAV_FAILED] request failed permanently | url={url} | error={type(error).__name__}: {error}'
            )

        # robots.txt fetch failed (e.g. 404) - fall back to the conventional sitemap.xml.
        if label == 'robots' and store_domain:
            parts = urlsplit(url)
            fallback = urlunsplit((parts.scheme, parts.netloc, '/sitemap.xml', '', ''))
            Actor.log.info(f'[SITEMAP] robots.txt unavailable, falling back to {fallback} | store={store_domain}')
            try:
                await context.add_requests([
                    Request.from_url(
                        fallback,
                        label='sitemap',
                        user_data={'page_type': 'sitemap', 'store_domain': store_domain, 'depth': 1},
                    )
                ])
                if fallback not in state.visited_urls:
                    state.visited_urls.add(fallback)
                    state.pages_pending[store_domain] += 1
            except Exception as exc:
                log_step_error('robots.sitemap_fallback', exc, url=fallback, tag='NAV_ERROR')

    async def on_skipped(self, request: Any, reason: str) -> None:
        """Called for requests skipped by the crawler (e.g. robots.txt restrictions)."""
        self.state.robots_skipped += 1
        url = request if isinstance(request, str) else request.url
        Actor.log.info(f'[ROBOTS] request skipped by robots.txt policy | url={url} | reason={reason}')


def _store_pages_remaining_guard(runtime: CrawlRuntime, store_domain: str) -> bool:
    """True when the per-store page budget (used + pending) is exhausted."""
    state = runtime.state
    return (
        state.pages_used[store_domain] + state.pages_pending[store_domain]
        >= runtime.config.max_pages_per_store
    )


def log_run_summary(state: CrawlState, config: CrawlConfig, stores_ok: set[str]) -> None:
    """Log the final run summary required by the spec (logs only, never billable records)."""
    pages_visited = sum(state.pages_used.values())
    summary = {
        'stores_attempted': len(config.store_domains),
        'stores_extracted': len(stores_ok),
        'pages_visited': pages_visited,
        'product_pages_processed': state.product_pages_processed,
        'unique_products_published': state.published,
        'duplicate_products_skipped': state.duplicates,
        'parsing_errors': state.parse_errors,
        'restricted_or_rate_limited_domains': len(state.restricted_domains),
        'restricted_domains': sorted(state.restricted_domains),
        'robots_skipped_requests': state.robots_skipped,
        'failed_navigation_attempts': state.nav_errors,
        'not_found_pages': state.not_found,
        'products_limit_reached': state.products_limit_hit,
        'charging_limit_reached': state.charge_limit_hit,
    }
    Actor.log.info('[SUMMARY] ' + json.dumps(summary))
    for domain, reason in sorted(state.restricted_domains.items()):
        Actor.log.warning(f'[SUMMARY] restricted domain: {domain} (reason={reason})')
    if state.charge_limit_hit:
        Actor.log.warning('[SUMMARY] a charging limit was reached - the crawl stopped gracefully.')
    if state.products_limit_hit:
        Actor.log.info(f"[SUMMARY] the configured product limit ({config.max_products}) was reached.")
    if state.published == 0 and not state.product_pages_processed:
        Actor.log.warning(
            '[SUMMARY] no product pages were processed - the dataset is empty. This run did NOT '
            'successfully extract a store; do not interpret it as full catalog coverage.'
        )
    elif state.published == 0 and state.product_pages_processed:
        Actor.log.warning(
            '[SUMMARY] product pages were processed but none produced a publishable record; '
            'check [PARSE_ERROR]/[NOT_SHOPIFY] log lines for causes.'
        )


# --------------------------------------------------------------------------------------
# Pricing model detection
# --------------------------------------------------------------------------------------


def detect_pricing_model() -> tuple[bool, int | None]:
    """Detect the active Apify pricing model: pay-per-event or legacy pay-per-result."""
    pay_per_event = False
    try:
        pay_per_event = bool(Actor.get_charging_manager().get_pricing_info().is_pay_per_event)
    except Exception as exc:
        Actor.log.warning(
            f'[PRICING] could not read pricing info ({type(exc).__name__}: {exc}); '
            f'assuming legacy pay-per-result path'
        )
    legacy_max: int | None = None
    try:
        legacy_max = Actor.configuration.max_paid_dataset_items
    except Exception:
        legacy_max = None
    return pay_per_event, legacy_max


async def read_existing_dataset_items() -> int:
    """Count items already in the default dataset (resumed/restarted runs must not re-fill it)."""
    try:
        dataset = await Actor.open_dataset()
        try:
            metadata = await dataset.get_metadata()
            return int(metadata.item_count or 0)
        except Exception:
            page = await dataset.get_data(offset=0, limit=1)
            return int(getattr(page, 'total', 0) or getattr(page, 'count', 0) or 0)
    except Exception as exc:
        Actor.log.warning(
            f'[PRICING] could not read the existing dataset item count '
            f'({type(exc).__name__}: {exc}); assuming 0 for this run'
        )
        return 0


# --------------------------------------------------------------------------------------
# Actor entry point
# --------------------------------------------------------------------------------------


async def main() -> None:
    """Actor lifecycle: validate input, configure the crawler, run, publish, summarize."""
    async with Actor:
        # ---- 1. Validate input ----
        try:
            raw_input = await Actor.get_input() or {}
            config = validate_input(raw_input)
        except InputValidationError as exc:
            Actor.log.error(f'[VALIDATION] input rejected: {exc}')
            raise
        except Exception as exc:
            log_step_error('main.get_input', exc, tag='VALIDATION')
            raise

        state = CrawlState()
        pay_per_event, legacy_max_paid = detect_pricing_model()
        if pay_per_event:
            Actor.log.info(
                f'[PRICING] pay-per-event mode active - each unique published product is '
                f'charged as event {CHARGED_EVENT_NAME!r} exactly once'
            )
        elif legacy_max_paid and legacy_max_paid > 0:
            Actor.log.info(f'[PRICING] legacy pay-per-result mode - max paid dataset items={legacy_max_paid}')
        else:
            Actor.log.info(
                '[PRICING] no legacy paid-item cap is configured (local/free run) - '
                'the configured maxProducts limit is the only output cap enforced'
            )
        legacy_initial_items = 0 if pay_per_event else await read_existing_dataset_items()
        if legacy_initial_items:
            Actor.log.info(f'[PRICING] existing default dataset items accounted for: {legacy_initial_items}')

        # ---- 2. Configure the proxy from the input (never logged, never altered on blocks) ----
        proxy_configuration = None
        try:
            proxy_configuration = await Actor.create_proxy_configuration(
                actor_proxy_input=raw_input.get('proxyConfiguration'),
            )
        except Exception as exc:
            if raw_input.get('proxyConfiguration'):
                Actor.log.error(
                    f'[PROXY_ERROR] proxy configuration was requested but could not be '
                    f'initialized: {type(exc).__name__}: {exc} - failing the run instead of '
                    f'silently degrading the proxy settings'
                )
                raise
            Actor.log.warning(f'[PROXY] no proxy requested; proceeding without a proxy ({type(exc).__name__})')
            proxy_configuration = None

        # ---- 3. Build the crawler and runtime ----
        runtime = CrawlRuntime(
            config, state, crawler=None,  # type: ignore[arg-type]  (assigned right below)
            pay_per_event=pay_per_event,
            legacy_max_paid_items=legacy_max_paid,
            legacy_initial_items=legacy_initial_items,
        )
        request_queue = await RequestQueue.open()
        throttled_manager = ThrottlingRequestManager(
            inner=request_queue,
            domains=[target.host for target in config.targets],
            request_manager_opener=RequestQueue.open,
        )
        concurrency_settings = ConcurrencySettings(
            min_concurrency=1,
            max_concurrency=config.max_concurrency,
            desired_concurrency=config.max_concurrency,
            max_tasks_per_minute=config.max_tasks_per_minute,
        )
        crawler = PlaywrightCrawler(
            request_handler=runtime.handle_request,
            request_manager=throttled_manager,
            proxy_configuration=proxy_configuration,
            max_requests_per_crawl=config.max_requests_per_crawl,
            max_request_retries=2,  # bounded retries for recoverable failures
            max_session_rotations=4,
            retry_on_blocked=False,  # never attempt automatic bot-protection bypass
            respect_robots_txt_file=True,
            concurrency_settings=concurrency_settings,
            request_handler_timeout=timedelta(seconds=90),
            navigation_timeout=timedelta(seconds=60),
            headless=True,
            keep_alive=False,
        )
        runtime.crawler = crawler
        crawler.error_handler(runtime.on_error)
        crawler.failed_request_handler(runtime.on_failed)
        crawler.on_skipped_request(runtime.on_skipped)

        initial_requests = [
            Request.from_url(
                target.url,
                label=target.kind,
                user_data={'page_type': target.kind, 'store_domain': target.store_domain, 'origin_start': True},
            )
            for target in config.targets
        ]
        # Book the initial targets into the visited/pending budget bookkeeping.
        for target in config.targets:
            state.visited_urls.add(target.url)
            state.pages_pending[target.store_domain] += 1
            if target.kind == 'product':
                state.products_pending[target.store_domain] += 1

        # ---- 4. Run the crawl ----
        try:
            await crawler.run(initial_requests)
        except PersistentRateLimitError as exc:
            Actor.log.warning(
                f'[RATE_LIMIT] the crawl stopped because a domain stayed rate-limited for '
                f'too long (spec: stop crawling that domain rather than hammering it): {exc}'
            )
        except Exception as exc:
            log_step_error('crawler.run', exc, tag='RUN_ERROR')
            raise

        # ---- 5. Final run summary (logs only - never mixed into the billable dataset) ----
        log_run_summary(state, config, runtime.stores_ok)


if __name__ == '__main__':
    asyncio.run(main())

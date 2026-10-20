# 🛍️ Shopify Product & Pricing Feed Extractor (AI & MCP Ready)

Extract clean, normalized product catalogs, pricing, variants, SKUs, image URLs, and **AI-ready Markdown** from any publicly accessible Shopify storefront, collection, or product URL.

Built for **AI Shopping Agents, RAG pipelines, E-commerce Competitor Price Tracking, and Catalog Analytics**.

---

## 🚀 Key Features

- **Full Storefront & Collection Discovery:** Automatically discovers products via `robots.txt` sitemaps, nested sitemap indexes, and paginated `/collections/` pages.
- **3-Tier Precision Extraction:** Combines `application/ld+json` structured data, embedded Shopify product JSON, and DOM metadata fallbacks so you never miss variants or prices.
- **Exact Variant & Pricing Math:** Extracts active selling prices, compare-at (original) prices, currency ISO codes, SKUs, barcodes, and stock availability using exact decimal math.
- **AI-Friendly Markdown Export:** Generates a clean, token-efficient Markdown summary per product (stripped of cookie banners, scripts, and nav menus)—ready to feed directly into LLMs, vector databases, or MCP agents.
- **Strict Budget & Rate-Limit Safety:** Respects `robots.txt`, handles pagination loops safely, and enforces strict `maxProducts` and `maxPagesPerStore` guardrails.

---

## 📥 Input Parameters

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `startUrls` | Array | *Required* | Public Shopify store homepages, collection URLs (`/collections/...`), or direct product URLs (`/products/...`). |
| `maxProducts` | Integer | `1000` | Maximum unique products to extract across the run. |
| `maxPagesPerStore` | Integer | `100` | Maximum crawled pages per store domain. |
| `includeVariants` | Boolean | `true` | Include full variant breakdown (IDs, SKUs, options, prices, stock state). |
| `includeMarkdown` | Boolean | `true` | Include clean LLM-ready Markdown representation in each record. |

---

## 📤 Sample Output (JSON)

```json
{
  "schema_version": "1.0",
  "record_type": "shopify_product",
  "product_key": "kyliecosmetics.com:cashmere-muse-eau-de-parfum",
  "product_id": "9175025090802",
  "store_domain": "kyliecosmetics.com",
  "canonical_url": "[https://kyliecosmetics.com/products/cashmere-muse-eau-de-parfum](https://kyliecosmetics.com/products/cashmere-muse-eau-de-parfum)",
  "title": "Cashmere Muse Eau De Parfum",
  "vendor": "kylie-fragrance",
  "currency": "USD",
  "price_min": "68.00",
  "price_max": "68.00",
  "available": true,
  "variants": [
    {
      "variant_id": "49834184638706",
      "price": "68.00",
      "sku": "KF161"
    }
  ],
  "markdown": "# Cashmere Muse Eau De Parfum\n\n**Brand:** kylie-fragrance\n**Price:** USD 68.00..."
}

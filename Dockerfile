FROM apify/actor-python-playwright:3.13-1.63.0

COPY requirements.txt ./

RUN pip install -U pip setuptools \
    && pip install -r requirements.txt \
    && pip install --no-deps --force-reinstall "playwright==1.63.0"

COPY . ./

CMD ["python3", "-u", "main.py"]

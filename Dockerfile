FROM apify/actor-python-playwright:3.13-1.63.0

COPY requirements.txt ./

RUN echo "Python version:" \
    && python --version \
    && echo "Installing dependencies:" \
    && pip install -U pip setuptools \
    && pip install -r requirements.txt \
    && echo "All installed Python packages:" \
    && pip freeze

RUN pip install --no-deps --force-reinstall "playwright==1.63.0"

COPY . ./

CMD ["sh", "-c", "if [ -f my_actor/main.py ]; then python my_actor/main.py; elif [ -f src/main.py ]; then python src/main.py; else python main.py; fi"]

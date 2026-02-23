FROM python:3.13-slim

RUN pip install --upgrade pip

ADD services/preprocessor/requirements.txt /app/services/preprocessor/requirements.txt

WORKDIR /app

ENV PYTHONPATH="/app"

RUN pip install -r services/preprocessor/requirements.txt --no-cache-dir

ADD . /app

CMD ["bash", "services/preprocessor/run.sh" ]
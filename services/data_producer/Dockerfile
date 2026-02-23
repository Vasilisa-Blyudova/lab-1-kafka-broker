FROM python:3.13-slim

RUN pip install --upgrade pip

ADD services/data_producer/requirements.txt /app/services/data_producer/requirements.txt

WORKDIR /app

ENV PYTHONPATH="/app"

RUN pip install -r services/data_producer/requirements.txt --no-cache-dir

ADD . /app

CMD ["bash", "services/data_producer/run.sh" ]
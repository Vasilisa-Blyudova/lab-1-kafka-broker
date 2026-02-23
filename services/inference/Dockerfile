FROM python:3.13-slim

RUN pip install --upgrade pip

ADD services/inference/requirements.txt /app/services/inference/requirements.txt

WORKDIR /app

ENV PYTHONPATH="/app"

RUN pip install -r services/inference/requirements.txt --no-cache-dir

ADD . /app

CMD ["bash", "services/inference/run.sh" ]
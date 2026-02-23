FROM python:3.13-slim

RUN pip install --upgrade pip

ADD client/requirements.txt /app/client/requirements.txt

WORKDIR /app

ENV PYTHONPATH="/app"

RUN pip install -r client/requirements.txt --no-cache-dir

ADD . /app

CMD ["bash", "client/run.sh" ]
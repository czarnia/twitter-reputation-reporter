FROM python:3.7-alpine

RUN pip3 install pika

COPY ./src /twitter_reporter/src
WORKDIR /twitter_reporter/src/date_agregator
ENTRYPOINT python3 -u date_agregator.py

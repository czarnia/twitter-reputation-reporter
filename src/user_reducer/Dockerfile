FROM python:3.7-alpine

RUN pip3 install pika

COPY ./src /twitter_reporter/src
WORKDIR /twitter_reporter/src/user_reducer
ENTRYPOINT python3 -u user_reducer.py

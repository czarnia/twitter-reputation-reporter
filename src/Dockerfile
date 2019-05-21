FROM python:3.7-alpine

RUN pip3 install nltk
RUN pip3 install pika
RUN pip3 install python-dateutil

COPY ./src /twitter_reporter/src
WORKDIR /twitter_reporter/src
RUN python3 -u download-nltk.py
ENTRYPOINT python3 -u main.py

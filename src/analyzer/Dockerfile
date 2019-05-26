FROM python:3.7-alpine

RUN pip3 install nltk
RUN pip3 install pika

COPY . /twitter_reporter
WORKDIR /twitter_reporter/src/analyzer
RUN python3 -u download-nltk.py
ENTRYPOINT python3 -u analyzer.py

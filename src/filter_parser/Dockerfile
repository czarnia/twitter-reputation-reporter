FROM python:3.7-alpine

RUN pip3 install pika
RUN pip3 install python-dateutil

COPY . /twitter_reporter
WORKDIR /twitter_reporter/src/filter_parser

ENTRYPOINT python3 -u filter_parser.py

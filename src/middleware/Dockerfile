FROM python:3.7-alpine

COPY . /middleware
RUN pip3 install pika && \
    export PYTHONPATH=$PYTHONPATH:/middleware
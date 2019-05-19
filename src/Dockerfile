FROM python:3.7-alpine
RUN pip3 install nltk
RUN pip3 install pika
COPY ./src /twitter_reporter/src
WORKDIR /twitter_reporter/src
RUN ls
ENTRYPOINT sh reporter-up.sh

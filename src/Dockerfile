FROM python:3.7-alpine
RUN pip3 install nltk
COPY ./ /src
WORKDIR /src
ENTRYPOINT python3 -u main.py

FROM middleware-base

RUN pip3 install python-dateutil

COPY . /twitter_reporter
WORKDIR /twitter_reporter
ENTRYPOINT python3 -u filter_parser.py

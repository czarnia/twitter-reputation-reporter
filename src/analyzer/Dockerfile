FROM middleware-base

RUN pip3 install nltk

COPY . /twitter_reporter
WORKDIR /twitter_reporter
RUN python3 -u download-nltk.py
ENTRYPOINT python3 -u analyzer.py

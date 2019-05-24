import os
import time
import random

from filter_parser import FilterParser
from analyzer import TwitterTextSentimentAnalyzer
from reducers import UserReducer, DateReducer
from date_agregator import DateAgregator
from middleware.rabbitmq_queues import RabbitMQQueues

SEND_QUEUE_NAME = "raw_twits"
RABBITMQ_HOST = 'rabbitmq'

class TwitterReputationReporter(object):
    def __init__(self, rabbitmq_queues):
        self.file_path = file_path
        self.queues =

    def start(self):
        print("------------------Entre al main--------------------")

        with open(self.file_path, "r") as twits:
            next(twits) #avoid header
            for line in twits:
                self.queues.send(line, random.random())

        self.queues.send_eom()

        print("------------------Sali del main--------------------")


if __name__ == '__main__':
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])

    rabbitmq_queues = RabbitMQQueues(SEND_QUEUE_NAME, RABBITMQ_HOST, filter_parser_workers)

    reporter = TwitterReputationReporter(rabbitmq_queues)
    reporter.start()

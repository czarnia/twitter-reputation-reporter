import os
import random
import sys
sys.path.append('../')

from middleware.rabbitmq_queues import RabbitMQQueues

SEND_QUEUE_NAME = "raw_twits"
RABBITMQ_HOST = 'rabbitmq'

class TwitterReputationReporter(object):
    def __init__(self, file_path, rabbitmq_queues):
        self.file_path = file_path
        self.queues = rabbitmq_queues

    def start(self):
        print("------------------Entre al main--------------------")

        with open(self.file_path, "r") as twits:
            next(twits) #avoid header
            for line in twits:
                self.queues.send(line, random.random())

        self.queues.send_eom()

        print("------------------Sali del main--------------------")


if __name__ == '__main__':
    file_path = os.environ['TWITS_FILE']
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])

    rabbitmq_queues = RabbitMQQueues(SEND_QUEUE_NAME, rabbitmq_host, filter_parser_workers)

    reporter = TwitterReputationReporter(file_path, rabbitmq_queues)
    reporter.start()

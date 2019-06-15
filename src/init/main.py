import logging

import os
import time
import random
import sys
sys.path.append('../')

from middleware.rabbitmq_queues import RabbitMQQueues
from middleware.log import config_log

SEND_QUEUE_NAME = "raw_twits"
RABBITMQ_HOST = 'rabbitmq'

LOG_FREQUENCY = 1000

class TwitterReputationReporter(object):
    def __init__(self, file_path, rabbitmq_queues):
        self.file_path = file_path
        self.queues = rabbitmq_queues

    def start(self):
        with open(self.file_path, "r") as twits:
            next(twits) #avoid header
            line_number = 0
            
            for line in twits:
                if (line_number % LOG_FREQUENCY == 0):
                    logging.info("Sending line [%d] %s", line_number, line)
                self.queues.send(line, line)
                line_number += 1

        logging.info("Sending EOM")
        self.queues.send_eom()


if __name__ == '__main__':
    config_log("INIT")

    file_path = os.environ['TWITS_FILE']
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])

    rabbitmq_queues = RabbitMQQueues(SEND_QUEUE_NAME, rabbitmq_host, filter_parser_workers)
    logging.info("Queues (%d) created", filter_parser_workers)

    reporter = TwitterReputationReporter(file_path, rabbitmq_queues)
    logging.info("Worker created, started running")
    reporter.start()
    logging.info("Worker finished, exiting")

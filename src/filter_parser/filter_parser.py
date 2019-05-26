import logging

import multiprocessing
import os
import sys
sys.path.append('../')

from dateutil.parser import parse

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.rabbitmq_queues import RabbitMQQueues
from middleware.log import config_log

AUTHOR_ID = 1
INBOUND = 2
CREATED_AT = 3
TEXT = 4

NUM_COLUMS = 7

IS_CUSTOMER = "True"

RECEIVE_QUEUE_NAME = "preprocesed_twits"
SEND_QUEUE_NAME = "raw_twits"

class FilterParser(multiprocessing.Process):
    def __init__(self, send_queues, receive_queue):
        multiprocessing.Process.__init__(self)
        self.send_queues = send_queues
        self.receive_queue = receive_queue

    def run(self):
        logging.info("Start consuming")
        self.receive_queue.consume(self._callback)
        logging.info("Sending EOM to queues")
        self.send_queues.send_eom()
        logging.info("Finish")

    def _callback(self, ch, method, properties, body):
        logging.info("Received {}".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').rstrip().split(",")

        if (len(body_values) != NUM_COLUMS) or (body_values[INBOUND] != IS_CUSTOMER):
            logging.info("Twit discarted")
            return

        day = str(parse(body_values[CREATED_AT]).date())
        logging.info("Sending parsed value")
        self.send_queues.send("{},{},{}".format(body_values[AUTHOR_ID], day, body_values[TEXT]), body_values.decode('UTF-8'))

if __name__ == '__main__':
    config_log("FILTER PARSER")

    rabbitmq_host = os.environ['RABBITMQ_HOST']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])

    workers = []

    for i in range(filter_parser_workers):
        send_queues = RabbitMQQueues(RECEIVE_QUEUE_NAME, rabbitmq_host, analyzer_workers)
        receive_queue = RabbitMQQueue("{}{}".format(SEND_QUEUE_NAME, i), rabbitmq_host)
        workers.append(FilterParser(send_queues, receive_queue))

    logging.info("Workers created")

    for i in range(filter_parser_workers):
        workers[i].start()

    logging.info("Starting running workers")
    logging.info("Waiting for workers to stop")
    for i in range(filter_parser_workers):
        workers[i].join()

    logging.info("All workers finished, exiting")

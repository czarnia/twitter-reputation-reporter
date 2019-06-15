import logging

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

LOG_FREQUENCY = 1000

class FilterParser(object):
    def __init__(self, send_queues, receive_queue):
        self.send_queues = send_queues
        self.receive_queue = receive_queue
        self.log_counter = 0

    def run(self):
        logging.info("Start consuming")
        self.receive_queue.consume(self._callback)
        logging.info("Sending EOM to queues")
        self.send_queues.send_eom()
        logging.info("Finish")

    def _callback(self, ch, method, properties, body):
        decoded_body = body.decode('UTF-8')

        if (self.log_counter % LOG_FREQUENCY == 0):
            logging.info("Received line [%d] %s", self.log_counter, decoded_body)

        body_values = decoded_body.rstrip().split(",")

        if (len(body_values) != NUM_COLUMS) or (body_values[INBOUND] != IS_CUSTOMER):
            if (self.log_counter % LOG_FREQUENCY == 0):
                logging.info("Twit discarted")
            self.log_counter += 1
            return

        day = str(parse(body_values[CREATED_AT]).date())

        if (self.log_counter % LOG_FREQUENCY == 0):
            logging.info("Sending parsed value")
        
        self.send_queues.send("{},{},{}".format(body_values[AUTHOR_ID], day, body_values[TEXT]), body.decode('UTF-8'))
        self.log_counter += 1

if __name__ == '__main__':
    config_log("FILTER PARSER")

    rabbitmq_host = os.environ['RABBITMQ_HOST']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])

    worker_id = int(os.environ['SERVICE_ID'])

    send_queues = RabbitMQQueues(RECEIVE_QUEUE_NAME, rabbitmq_host, analyzer_workers)
    receive_queue = RabbitMQQueue("{}{}".format(SEND_QUEUE_NAME, worker_id), rabbitmq_host)
    worker = FilterParser(send_queues, receive_queue)

    logging.info("Worker created, started running")
    worker.run()
    logging.info("Worker finished, exiting")

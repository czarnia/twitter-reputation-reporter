import logging

import os
import sys
sys.path.append('../')

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.log import config_log

DATE = 0
SCORE = 1

FLUSH_VALUE = 10

NEGATIVE_SCORE = -1

POSITIVE = "positive"
NEGATIVE = "negative"

DATE_RECEIVE_QUEUE_NAME = "date_twits"
DATE_SEND_QUEUE_NAME = "date_processed_twits"

class DateReducer(object):
    def __init__(self, receive_rabbitmq_queue, send_rabbitmq_queue):
        multiprocessing.Process.__init__(self)
        self.receive_rabbitmq_queue = receive_rabbitmq_queue
        self.send_rabbitmq_queue = send_rabbitmq_queue
        self.received = 0
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        self.received += 1
        logging.info("Received {}".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { POSITIVE : 0, NEGATIVE : 0 }

        if int(body_values[SCORE]) == NEGATIVE_SCORE:
            self.dates[body_values[DATE]][NEGATIVE] += 1
        else:
            self.dates[body_values[DATE]][POSITIVE] += 1

        logging.info("Dates info {}".format(self.dates))

        if self.received != FLUSH_VALUE:
            return

        for date in self.dates:
            logging.info("Sending: date = {}, positives = {}, negatives = {}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))

        self.dates = {}
        self.received = 0


    def run(self):
        logging.info("Start consuming")
        self.receive_rabbitmq_queue.consume(self._callback)
        logging.info("Stopped consuming, dates info obtained {}".format(self.dates))
        for date in self.dates:
            logging.info("Sending: date = {}, positives = {}, negatives = {}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
        logging.info("Sending EOM to queues")
        self.send_rabbitmq_queue.send_eom()
        logging.info("Finish")

if __name__ == '__main__':
    config_log("DATE REDUCER")

    rabbitmq_host = os.environ['RABBITMQ_HOST']
    date_reducer_workers = int(os.environ['DATE_REDUCER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])
    
    worker_id = int(os.environ['SERVICE_ID'])

    send_rabbitmq_queue = RabbitMQQueue(DATE_SEND_QUEUE_NAME, rabbitmq_host)
    receive_rabbitmq_queue = RabbitMQQueue("{}{}".format(DATE_RECEIVE_QUEUE_NAME, worker_id), rabbitmq_host, analyzer_workers)
    worker = DateReducer(receive_rabbitmq_queue, send_rabbitmq_queue)

    logging.info("Worker created, started running")
    worker.run()
    logging.info("Worker finished, exiting")

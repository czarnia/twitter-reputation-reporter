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

LOG_FREQUENCY = 1000

class DateReducer(object):
    def __init__(self, receive_rabbitmq_queue, send_rabbitmq_queue):
        self.receive_rabbitmq_queue = receive_rabbitmq_queue
        self.send_rabbitmq_queue = send_rabbitmq_queue
        self.received = 0
        self.log_counter = 0
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        self.received += 1

        decoded_body = body.decode('UTF-8')

        if (self.log_counter % LOG_FREQUENCY == 0):
            logging.info("Received line [%d] %s", self.log_counter, decoded_body)
        self.log_counter += 1

        body_values = decoded_body.split(",")

        date = body_values[DATE]
        score = body_values[SCORE]

        if not date in self.dates:
            self.dates[date] = { POSITIVE : 0, NEGATIVE : 0 }

        if int(score) == NEGATIVE_SCORE:
            self.dates[date][NEGATIVE] += 1
        else:
            self.dates[date][POSITIVE] += 1

        if self.received != FLUSH_VALUE:
            return

        for date in self.dates:
            if (self.log_counter % LOG_FREQUENCY == 0):
                logging.info("Sending: date = %s, positives = %s, negatives = %s", date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE])
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))

        self.dates = {}
        self.received = 0


    def run(self):
        logging.info("Start consuming")
        self.receive_rabbitmq_queue.consume(self._callback)
        logging.info("Stopped consuming, dates info obtained {}".format(self.dates))
        for date in self.dates:
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

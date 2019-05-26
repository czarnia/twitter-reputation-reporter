import logging
import os
import sys
sys.path.append('../')

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.log import config_log

DATE = 0
POSITIVES = 1
NEGATIVES = 2

POSITIVE = "positive"
NEGATIVE = "negative"

RECEIVE_QUEUE_NAME = "date_processed_twits"
DATE_REPORT_FILE = "/twitter_reporter/reports/dates_report.csv"

class DateAggregator(object):
    def __init__(self, rabbitmq_queue):
        self.rabbitmq_queue = rabbitmq_queue
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        logging.info("Received {}".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { POSITIVE : 0, NEGATIVE : 0 }

        self.dates[body_values[DATE]][NEGATIVE] += int(body_values[NEGATIVES])
        self.dates[body_values[DATE]][POSITIVE] += int(body_values[POSITIVES])
        logging.info("Dates info: {}".format(self.dates))

    def start(self):
        logging.info("Start consuming")
        self.rabbitmq_queue.consume(self._callback)

        logging.info("Stoped consuming, dates info obtained: {}".format(self.dates))
        dates = list(self.dates.keys())
        dates.sort()

        logging.info("Writting dates report")
        with open(DATE_REPORT_FILE, mode='w') as report:
            report.write("DATE, POSITIVES, NEGATIVES\n")
            for date in dates:
                logging.info("Writing {},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
                report.write("{},{},{}\n".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))


if __name__ == '__main__':
    config_log("DATE AGGREGATOR")
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])

    rabbitmq_queue = RabbitMQQueue(RECEIVE_QUEUE_NAME, rabbitmq_host, user_reduce_workers)
    logging.info("Queue created")

    date_aggregator = DateAggregator(rabbitmq_queue)
    logging.info("Starting worker")
    logging.info("Waiting for worker to stop")
    date_aggregator.start()
    logging.info("Worker finished, exiting")

import logging

import os
import fcntl
import sys
sys.path.append('../')

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.log import config_log

AUTHOR_ID = 0
SCORE = 1

ALERT_NUMBER = 3
NEGATIVE_SCORE = -1

USR_REPORT_FILE = "/twitter_reporter/reports/user_report.csv"

USR_RECEIVE_QUEUE_NAME = "usr_twits"

class UserReducer(object):
    def __init__(self, rabbitmq_queue):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = rabbitmq_queue
        self.users = {}

    def _was_already_alerted(self, author_id):
        return (author_id in self.users and self.users[author_id] == ALERT_NUMBER)

    def _callback(self, ch, method, properties, body):
        logging.info("Received {}".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').split(",")
        if int(body_values[SCORE]) != NEGATIVE_SCORE or self._was_already_alerted(body_values[AUTHOR_ID]):
            logging.info("Skipping value since it does not have a negative score or was already alerted")
            return

        if body_values[AUTHOR_ID] in self.users:
            self.users[body_values[AUTHOR_ID]] += 1
        else:
            self.users[body_values[AUTHOR_ID]] = 1

        logging.info("User info {}".format(self.users))

        if self.users[body_values[AUTHOR_ID]] == ALERT_NUMBER:
            logging.info("Reporting on user = {}".format(body_values[AUTHOR_ID]))
            with open(USR_REPORT_FILE, mode='a') as report:
                fcntl.flock(report, fcntl.LOCK_EX)
                report.write("{}\n".format(body_values[AUTHOR_ID]))
                fcntl.flock(report, fcntl.LOCK_UN)

    def run(self):
        logging.info("Starting consuming")
        self.rabbitmq_queue.consume(self._callback)
        logging.info("Stopped consuming, exiting")

if __name__ == '__main__':
    config_log("USER REDUCER")
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    user_reducer_workers = int(os.environ['USER_REDUCER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])

    worker_id = int(os.environ['SERVICE_ID'])

    rabbitmq_queue =  RabbitMQQueue("{}{}".format(USR_RECEIVE_QUEUE_NAME, worker_id), rabbitmq_host, analyzer_workers)
    worker = UserReducer(rabbitmq_queue)

    logging.info("Worker created, started running")
    worker.run()
    logging.info("Worker finished, exiting")

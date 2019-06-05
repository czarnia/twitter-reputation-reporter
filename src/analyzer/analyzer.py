import logging

import os
import sys
sys.path.append('../')

from nltk.sentiment.vader import SentimentIntensityAnalyzer

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.rabbitmq_queues import RabbitMQQueues
from middleware.log import config_log

AUTHOR_ID = 0
CREATED_AT = 1
TEXT = 2

BASE_NEGATIVE_SCORE = -0.5
BASE_POSITIVE_SCORE = 0.5

NEGATIVE_SCORE = -1
POSITIVE_SCORE = 1

RECEIVE_QUEUE_NAME = "preprocesed_twits"
SEND_USR_QUEUE_NAME = "usr_twits"
SEND_DATE_QUEUE_NAME = "date_twits"


class TwitterTextSentimentAnalyzer(object):
    def __init__(self, receive_queue, send_usr_queues, send_date_queues):
        self.receive_queue = receive_queue
        self.send_usr_queues = send_usr_queues
        self.send_date_queues = send_date_queues
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

    def _is_score_neutral(self, score):
        return (score < BASE_POSITIVE_SCORE and score > BASE_NEGATIVE_SCORE)

    def _map_score(self, score):
        return POSITIVE_SCORE if score >= BASE_POSITIVE_SCORE else NEGATIVE_SCORE

    def _callback(self, ch, method, properties, body):
        logging.info("Received {}".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').split(",")

        score = self.sentiment_analyzer.polarity_scores(body_values[TEXT])['compound']
        logging.info("Score is {}".format(score))

        if self._is_score_neutral(score):
            logging.info("The score is neutral")
            return

        score = self._map_score(score)

        logging.info("Sending: author_id = {}, date = {}, score = {}".format(body_values[AUTHOR_ID], body_values[CREATED_AT], score))
        self.send_usr_queues.send("{},{}".format(body_values[AUTHOR_ID], score), body_values[AUTHOR_ID])
        self.send_date_queues.send("{},{}".format(body_values[CREATED_AT], score), body_values[AUTHOR_ID])

    def run(self):
        logging.info("Start consuming")
        self.receive_queue.consume(self._callback)
        logging.info("Sending EOM to usr queues")
        self.send_usr_queues.send_eom()
        logging.info("Sending EOM to date queus")
        self.send_date_queues.send_eom()
        logging.info("Finish")

if __name__ == '__main__':
    config_log("ANALYZER")

    rabbitmq_host = os.environ['RABBITMQ_HOST']
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])
    date_reduce_workers = int(os.environ['DATE_REDUCER_WORKERS'])

    worker_id = int(os.environ['SERVICE_ID'])

    send_usr_queues = RabbitMQQueues(SEND_USR_QUEUE_NAME, rabbitmq_host, user_reduce_workers)
    send_date_queues = RabbitMQQueues(SEND_DATE_QUEUE_NAME, rabbitmq_host, date_reduce_workers)
    receive_queue = RabbitMQQueue("{}{}".format(RECEIVE_QUEUE_NAME, worker_id), rabbitmq_host, filter_parser_workers)
    worker = TwitterTextSentimentAnalyzer(receive_queue, send_usr_queues, send_date_queues)

    logging.info("Worker created, started running")

    worker.run()

    logging.info("Worker finished, exiting")

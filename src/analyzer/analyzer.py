import multiprocessing
import os
import sys
sys.path.append('../')

from nltk.sentiment.vader import SentimentIntensityAnalyzer

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.rabbitmq_queues import RabbitMQQueues

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


class TwitterTextSentimentAnalyzer(multiprocessing.Process):
    def __init__(self, receive_queue, send_usr_queues, send_date_queues):
        multiprocessing.Process.__init__(self)
        self.receive_queue = receive_queue
        self.send_usr_queues = send_usr_queues
        self.send_date_queues = send_date_queues

    def _is_score_neutral(self, score):
        return (score < BASE_POSITIVE_SCORE and score > BASE_NEGATIVE_SCORE)

    def _map_score(self, score):
        return POSITIVE_SCORE if score >= BASE_POSITIVE_SCORE else NEGATIVE_SCORE

    def _callback(self, ch, method, properties, body):
        sentiment_analyzer = SentimentIntensityAnalyzer()
        body_values = body.decode('UTF-8').split(",")
        score = sentiment_analyzer.polarity_scores(body_values[TEXT])['compound']

        if self._is_score_neutral(score):
            return

        score = self._map_score(score)

        self.send_usr_queues.send("{},{}".format(body_values[AUTHOR_ID], score), body_values[AUTHOR_ID])
        self.send_date_queues.send("{},{}".format(body_values[CREATED_AT], score), body_values[AUTHOR_ID])

    def run(self):
        print("------------------Entre al analyzer--------------------")
        self.receive_queue.consume(self._callback)
        self.send_usr_queues.send_eom()
        self.send_date_queues.send_eom()
        print("------------------Sali del analyzer--------------------")

if __name__ == '__main__':
    rabbitmq_host = os.environ['RABBITMQ_HOST']
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])
    date_reduce_workers = int(os.environ['DATE_REDUCER_WORKERS'])

    send_usr_queues = RabbitMQQueues(SEND_USR_QUEUE_NAME, rabbitmq_host, user_reduce_workers)
    send_date_queues = RabbitMQQueues(SEND_DATE_QUEUE_NAME, rabbitmq_host, date_reduce_workers)

    workers = []

    for i in range(analyzer_workers):
        receive_queue = RabbitMQQueue("{}{}".format(RECEIVE_QUEUE_NAME, i), rabbitmq_host, filter_parser_workers)
        workers.append(TwitterTextSentimentAnalyzer(receive_queue, send_usr_queues, send_date_queues))

    for i in range(analyzer_workers):
        workers[i].run()

    for i in range(analyzer_workers):
        workers[i].join()

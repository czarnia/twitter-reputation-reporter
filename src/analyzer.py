import multiprocessing
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


class TwitterTextSentimentAnalyzer(multiprocessing.Process):
    def __init__(self, id, num_filter_workers, num_usr_workers, num_date_workers):
        multiprocessing.Process.__init__(self)
        self.num_filter_workers = num_filter_workers
        self.receive_queue = RabbitMQQueue("preprocesed_twits{}".format(id), 'rabbitmq')
        self.send_usr_queues = RabbitMQQueues("usr_twits", 'rabbitmq', num_usr_workers)
        self.send_date_queues = RabbitMQQueues("date_twits", 'rabbitmq', num_date_workers)

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
        self.receive_queue.consume(self._callback, self.num_filter_workers)
        self.send_usr_queues.send_eom()
        self.send_date_queues.send_eom()

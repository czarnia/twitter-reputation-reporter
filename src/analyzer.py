import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue
from nltk.sentiment.vader import SentimentIntensityAnalyzer

AUTHOR_ID = 0
CREATED_AT = 1
TEXT = 2


class TwitterTextSentimentAnalyzer(multiprocessing.Process):
    def __init__(self, num_usr_workers, num_date_workers):
        multiprocessing.Process.__init__(self)
        self.receive_queue = RabbitMQQueue("preprocesed_twits", 'rabbitmq')
        self.send_usr_queues = [ RabbitMQQueue("usr_twits{}".format(i), 'rabbitmq') for i in range(num_usr_workers) ]
        self.send_date_queues = [ RabbitMQQueue("date_twits{}".format(i), 'rabbitmq') for i in range(num_date_workers) ]

    def _hash(self, value, max_range):
        return hash(value) % max_range

    def _callback(self, ch, method, properties, body):
        sentiment_analyzer = SentimentIntensityAnalyzer()
        body_values = body.decode('UTF-8').split(",")
        score = sentiment_analyzer.polarity_scores(body_values[TEXT])['compound']

        usr_queue = self.send_usr_queues[self._hash(body_values[AUTHOR_ID], len(self.send_usr_queues))]
        usr_queue.send("{},{}".format(body_values[AUTHOR_ID], score))

        date_queue = self.send_date_queues[self._hash(body_values[AUTHOR_ID], len(self.send_date_queues))]
        date_queue.send("{},{}".format(body_values[CREATED_AT], score))

    def run(self):
        self.receive_queue.consume(self._callback)

        print("")
        print("--------------ANALYZER, TERMINO DE CONSUMIR--------------")
        for queu in self.send_usr_queues:
            queu.send_eom()
        for queu in self.send_date_queues:
            queu.send_eom()
        print("")
        print("--------------ANALYZER, ENVIO LOS EOM--------------")

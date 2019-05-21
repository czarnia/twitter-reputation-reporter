import os

from filter_parser import FilterParser
from analyzer import TwitterTextSentimentAnalyzer
from reducers import UserReducer, DateReducer
from date_agregator import DateAgregator
from middleware.rabbitmq_queues import RabbitMQQueues

SEND_QUEUE_NAME = "raw_twits"
RABBITMQ_HOST = 'rabbitmq'

class TwitterReputationReporter(object):
    def __init__(self, rabbitmq_host, file_path, filter_parser_workers, analyzer_workers, user_reduce_workers, date_reduce_workers):
        self.file_path = file_path
        self.queues = RabbitMQQueues(SEND_QUEUE_NAME, RABBITMQ_HOST, filter_parser_workers)
        self.workers = []

        for i in range(filter_parser_workers):
            self.workers.append(FilterParser(i, rabbitmq_host, analyzer_workers))

        for i in range(analyzer_workers):
            self.workers.append(TwitterTextSentimentAnalyzer(i, rabbitmq_host, filter_parser_workers, user_reduce_workers, date_reduce_workers))

        for i in range(user_reduce_workers):
            self.workers.append(UserReducer(i, rabbitmq_host, analyzer_workers))

        for i in range(date_reduce_workers):
            self.workers.append(DateReducer(i, rabbitmq_host, analyzer_workers))

        self.workers.append(DateAgregator(rabbitmq_host, date_reduce_workers))

    def start(self):
        for worker in self.workers:
            worker.start()

        with open(self.file_path, "r") as twits:
            next(twits) #avoid header
            for line in twits:
                self.queues.send(line, line[0])

        self.queues.send_eom()

        for worker in self.workers:
            worker.join()


if __name__ == '__main__':
    twits_file = os.environ['TWITS_FILE']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])
    date_reduce_workers = int(os.environ['DATE_REDUCER_WORKERS'])

    reporter = TwitterReputationReporter(RABBITMQ_HOST, twits_file, filter_parser_workers, analyzer_workers, user_reduce_workers, date_reduce_workers)
    reporter.start()

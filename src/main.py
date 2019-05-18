import os

from filter_parser import FilterParser
from analyzer import TwitterTextSentimentAnalyzer
from middleware.rabbitmq_queue import RabbitMQQueue

class TwitterReputationReporter(object):
    def __init__(self, file_path, filter_parser_workers, analyzer_workers, user_reduce_workers, date_reduce_workers):
        self.file_path = file_path
        self.queue = RabbitMQQueue("raw_twits", "rabbitmq")
        self.workers = []

        for i in range(filter_parser_workers):
            self.workers.append(FilterParser())

        for i in range(analyzer_workers):
            self.workers.append(TwitterTextSentimentAnalyzer(user_reduce_workers, date_reduce_workers))

        for i in range(user_reduce_workers):
            self.workers.append(UserReducer())

        for i in range(date_reduce_workers):
            self.workers.append(DateReducer())

        self.workers.append(DateAgregateWorker())

    def start(self):
        for worker in self.workers:
            worker.start()

        with open(self.file_path, "r") as twits:
            next(twits) #avoid header
            for line in twits:
                self.queue.send(line)

        for worker in self.workers:
            worker.join()


if __name__ == '__main__':
    twits_file = os.environ['TWITS_FILE']
    filter_parser_workers = int(os.environ['FILTER_PARSER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])
    date_reduce_workers = int(os.environ['DATE_REDUCER_WORKERS'])

    reporter = TwitterReputationReporter(twits_file, filter_parser_workers, analyzer_workers, user_reduce_workers, date_reduce_workers)
    reporter.start()

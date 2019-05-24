import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

DATE = 0
POSITIVES = 1
NEGATIVES = 2

POSITIVE = "positive"
NEGATIVE = "negative"

RECEIVE_QUEUE_NAME = "date_processed_twits"
DATE_REPORT_FILE = "/twitter_reporter/reports/dates_report.csv"

class DateAgregator(multiprocessing.Process):
    def __init__(self, rabbitmq_queue):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = rabbitmq_queue
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { POSITIVE : 0, NEGATIVE : 0 }

        self.dates[body_values[DATE]][NEGATIVE] += int(body_values[NEGATIVES])
        self.dates[body_values[DATE]][POSITIVE] += int(body_values[POSITIVES])

    def run(self):
        print("------------------Entre al date agregator--------------------")
        self.rabbitmq_queue.consume(self._callback)

        dates = list(self.dates.keys())
        dates.sort()

        with open(DATE_REPORT_FILE, mode='w') as report:
            report.write("DATE, POSITIVES, NEGATIVES\n")
            for date in dates:
                report.write("{},{},{}\n".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
        print("------------------Sali del date agregator--------------------")


if __name__ == '__main__':
    user_reduce_workers = int(os.environ['USER_REDUCER_WORKERS'])
    rabbitmq_queue = RabbitMQQueue(RECEIVE_QUEUE_NAME, rabbitmq_host, user_reduce_workers)

    date_agregator = DateAgregator(rabbitmq_queue)
    date_agregator.run()
    date_agregator.join()

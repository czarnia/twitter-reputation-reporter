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
    def __init__(self, rabbitmq_host, number_of_producers):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = RabbitMQQueue(RECEIVE_QUEUE_NAME, rabbitmq_host)
        self.number_of_producers = number_of_producers
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { POSITIVE : 0, NEGATIVE : 0 }

        self.dates[body_values[DATE]][NEGATIVE] += int(body_values[NEGATIVES])
        self.dates[body_values[DATE]][POSITIVE] += int(body_values[POSITIVES])

    def run(self):
        self.rabbitmq_queue.consume(self._callback, self.number_of_producers)

        dates = list(self.dates.keys())
        dates.sort()

        with open(DATE_REPORT_FILE, mode='w') as report:
            report.write("DATE, POSITIVES, NEGATIVES\n")
            for date in dates:
                report.write("{},{},{}\n".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))

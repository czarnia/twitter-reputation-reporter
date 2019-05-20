import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

DATE = 0
POSITIVES = 1
NEGATIVES = 2

class DateAgregator(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = RabbitMQQueue("date_processed_twits", 'rabbitmq')
        self.report_file_name = "/twitter_reporter/reports/dates_report.csv"
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        print("------------DATE AGREGATOR ME LLEGO: {}-------------".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { "positive" : 0, "negative" : 0 }

        self.dates[body_values[DATE]]["negative"] += int(body_values[NEGATIVES])
        self.dates[body_values[DATE]]["positive"] += int(body_values[POSITIVES])

    def run(self):
        self.rabbitmq_queue.consume(self._callback)

        dates = list(self.dates.keys())
        dates.sort()

        with open(self.report_file_name, mode='w') as report:
            report.write("DATE, POSITIVES, NEGATIVES\n")
            for date in dates:
                report.write("{},{},{}\n".format(date, self.dates[date]["positive"], self.dates[date]["negative"]))

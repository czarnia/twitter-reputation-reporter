import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

class DateAgregator(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = RabbitMQQueue("date_processed_twits", 'rabbitmq')
        self.report_file_name = "dates_report.csv"
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        body_values = str(body).split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { "positive" : 0, "negative" : 0 }

        if body_values[SCORE] == NEGATIVE_SCORE:
            self.dates[body_values[DATE]]["negative"] += 1
        else:
            self.dates[body_values[DATE]]["positive"] += 1

    def run(self):
        self.rabbitmq_queue.consume(self._callback)

        dates = list(self.dates.keys())
        dates.sort()

        with open(self.report_file_name, mode='w') as report:
            report.write("DATE, POSITIVES, NEGATIVES\n")
            for date in dates:
                report.write("%s, %s, %s\n".format(date, self.dates[date]["positive"], self.dates[date]["negative"]))

import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 0
DATE = 0
SCORE = 1

ALERT_NUMBER = 3
NEGATIVE_SCORE = -1

class UserReducer(multiprocessing.Process):
    def __init__(self, id):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = RabbitMQQueue("usr_twits%i".format(id), "rabbitmq")
        self.users = {}
        self.report_file_name = "user_report.csv"

    def _was_already_alerted(self, author_id):
        return (author_id in self.users and self.users[author_id] == ALERT_NUMBER)

    def _callback(self, ch, method, properties, body):
        body_values = body.split(",")
        if body_values[SCORE] != NEGATIVE_SCORE or self._was_already_alerted(body_values[AUTHOR_ID]):
            return

        if body_values[AUTHOR_ID] in self.users:
            self.users[AUTHOR_ID] += 1
        else:
            self.users[AUTHOR_ID] = 1

        if body_values[AUTHOR_ID] == ALERT_NUMBER:
            with open(self.report_file_name, mode='a') as report:
                fcntl.flock(report, fcntl.LOCK_EX)
                report.write("%s\n".format(body_values[AUTHOR_ID]))
                fcntl.flock(report, fcntl.LOCK_UN)

    def run(self):
        self.rabbitmq_queue.consume(self._callback)

class DateReducer(multiprocessing.Process):
    def __init__(self, id):
        multiprocessing.Process.__init__(self)
        self.receive_rabbitmq_queue = RabbitMQQueue("date_twits%i".format(id), "rabbitmq")
        self.send_rabbitmq_queue = RabbitMQQueue("date_processed_twits", "rabbitmq")
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        body_values = body.split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { "positive" : 0, "negative" : 0 }

        if body_values[SCORE] == NEGATIVE_SCORE:
            self.dates[body_values[DATE]]["negative"] += 1
        else:
            self.dates[body_values[DATE]]["positive"] += 1


    def run(self):
        self.receive_rabbitmq_queue.consume(self._callback)
        for date in self.dates:
            self.send_rabbitmq_queue.send("%s,%s,%s".format(date, self.dates[date]["positive"], self.dates[date]["negative"]))

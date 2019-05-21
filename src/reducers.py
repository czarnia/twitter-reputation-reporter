import multiprocessing
import fcntl

from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 0
DATE = 0
SCORE = 1

ALERT_NUMBER = 3
NEGATIVE_SCORE = -1

POSITIVE = "positive"
NEGATIVE = "negative"

USR_REPORT_FILE = "/twitter_reporter/reports/user_report.csv"

USR_RECEIVE_QUEUE_NAME = "usr_twits"
DATE_RECEIVE_QUEUE_NAME = "date_twits"
DATE_SEND_QUEUE_NAME = "date_processed_twits"

class UserReducer(multiprocessing.Process):
    def __init__(self, id, rabbitmq_host, num_analyzer_workers):
        multiprocessing.Process.__init__(self)
        self.id = id
        self.rabbitmq_queue = RabbitMQQueue("{}{}".format(USR_RECEIVE_QUEUE_NAME, id), rabbitmq_host)
        self.num_analyzer_workers = num_analyzer_workers
        self.users = {}

    def _was_already_alerted(self, author_id):
        return (author_id in self.users and self.users[author_id] == ALERT_NUMBER)

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').split(",")
        if int(body_values[SCORE]) != NEGATIVE_SCORE or self._was_already_alerted(body_values[AUTHOR_ID]):
            return

        if body_values[AUTHOR_ID] in self.users:
            self.users[body_values[AUTHOR_ID]] += 1
        else:
            self.users[body_values[AUTHOR_ID]] = 1

        if self.users[body_values[AUTHOR_ID]] == ALERT_NUMBER:
            with open(USR_REPORT_FILE, mode='a') as report:
                fcntl.flock(report, fcntl.LOCK_EX)
                report.write("{}\n".format(body_values[AUTHOR_ID]))
                fcntl.flock(report, fcntl.LOCK_UN)

    def run(self):
        self.rabbitmq_queue.consume(self._callback, self.num_analyzer_workers)

class DateReducer(multiprocessing.Process):
    def __init__(self, id, rabbitmq_host, num_analyzer_workers):
        multiprocessing.Process.__init__(self)
        self.receive_rabbitmq_queue = RabbitMQQueue("{}{}".format(DATE_RECEIVE_QUEUE_NAME, id), rabbitmq_host)
        self.send_rabbitmq_queue = RabbitMQQueue(DATE_SEND_QUEUE_NAME, rabbitmq_host)
        self.num_analyzer_workers = num_analyzer_workers
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { POSITIVE : 0, NEGATIVE : 0 }

        if body_values[SCORE] == NEGATIVE_SCORE:
            self.dates[body_values[DATE]][NEGATIVE] += 1
        else:
            self.dates[body_values[DATE]][POSITIVE] += 1


    def run(self):
        self.receive_rabbitmq_queue.consume(self._callback, self.num_analyzer_workers)
        for date in self.dates:
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
        self.send_rabbitmq_queue.send_eom()

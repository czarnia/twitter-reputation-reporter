import multiprocessing
import fcntl

from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 0
SCORE = 1

ALERT_NUMBER = 3
NEGATIVE_SCORE = -1

USR_REPORT_FILE = "/twitter_reporter/reports/user_report.csv"

USR_RECEIVE_QUEUE_NAME = "usr_twits"

class UserReducer(multiprocessing.Process):
    def __init__(self, rabbitmq_queue):
        multiprocessing.Process.__init__(self)
        self.rabbitmq_queue = rabbitmq_queue
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
        print("------------------Entre al user reducer--------------------")
        self.rabbitmq_queue.consume(self._callback, self.num_analyzer_workers)
        print("------------------Sali del user reducer--------------------")

if __name__ == '__main__':
    user_reducer_workers = int(os.environ['USER_REDUCER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])

    workers = []

    for i in range(user_reducer_workers):
        rabbitmq_queue =  RabbitMQQueue("{}{}".format(USR_RECEIVE_QUEUE_NAME, i), rabbitmq_host, analyzer_workers)
        workers.append(UserReducer(rabbitmq_queue))

    for i in range(user_reducer_workers):
        workers[i].run()

    for i in range(user_reducer_workers):
        workers[i].join()

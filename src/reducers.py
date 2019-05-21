import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 0
DATE = 0
SCORE = 1

ALERT_NUMBER = 3
NEGATIVE_SCORE = -1

class UserReducer(multiprocessing.Process):
    def __init__(self, id, num_analyzer_workers):
        multiprocessing.Process.__init__(self)
        self.id = id
        self.rabbitmq_queue = RabbitMQQueue("usr_twits{}".format(id), 'rabbitmq')
        self.num_analyzer_workers = num_analyzer_workers
        self.users = {}
        self.report_file_name = "/twitter_reporter/reports/user_report.csv"

    def _was_already_alerted(self, author_id):
        return (author_id in self.users and self.users[author_id] == ALERT_NUMBER)

    def _callback(self, ch, method, properties, body):
        #print("--------------USER-REDUCER, recibo la linea: {}--------------".format(str(body)))
        body_values = body.decode('UTF-8').split(",")
        #print("ANTES DEL IF")
        if int(body_values[SCORE]) != NEGATIVE_SCORE or self._was_already_alerted(body_values[AUTHOR_ID]):
            return
        #print("DESPUES DEL IF")

        if body_values[AUTHOR_ID] in self.users:
            self.users[body_values[AUTHOR_ID]] += 1
        else:
            self.users[body_values[AUTHOR_ID]] = 1

        if self.users[body_values[AUTHOR_ID]] == ALERT_NUMBER:
            with open(self.report_file_name, mode='a') as report:
                fcntl.flock(report, fcntl.LOCK_EX)
                report.write("{}\n".format(body_values[AUTHOR_ID]))
                fcntl.flock(report, fcntl.LOCK_UN)

    def run(self):
        #print("")
        print("--------------USER-REDUCER, EMPIEZO--------------")
        self.rabbitmq_queue.consume(self._callback, self.num_analyzer_workers)
        #print("")
        #print("--------------USER-REDUCER, estado final: {}--------------".format(self.users))
        print("--------------USER-REDUCER, TERMINO--------------")
        print("\n")

class DateReducer(multiprocessing.Process):
    def __init__(self, id, num_analyzer_workers):
        multiprocessing.Process.__init__(self)
        self.receive_rabbitmq_queue = RabbitMQQueue("date_twits{}".format(id), 'rabbitmq')
        self.send_rabbitmq_queue = RabbitMQQueue("date_processed_twits", 'rabbitmq')
        self.num_analyzer_workers = num_analyzer_workers
        self.dates = {}

    def _callback(self, ch, method, properties, body):
        #print("--------------DATE-REDUCER, recibo la linea: {}--------------".format(str(body)))
        body_values = body.decode('UTF-8').split(",")

        if not body_values[DATE] in self.dates:
            self.dates[body_values[DATE]] = { "positive" : 0, "negative" : 0 }

        if body_values[SCORE] == NEGATIVE_SCORE:
            self.dates[body_values[DATE]]["negative"] += 1
        else:
            self.dates[body_values[DATE]]["positive"] += 1


    def run(self):
        print("------------------DATE REDUCER, EMPIEZO--------------------------")
        self.receive_rabbitmq_queue.consume(self._callback, self.num_analyzer_workers)
        for date in self.dates:
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date]["positive"], self.dates[date]["negative"]))
        self.send_rabbitmq_queue.send_eom()
        #print("")
        print("------------------DATE REDUCER, TERMINO-------------------")
        print("")

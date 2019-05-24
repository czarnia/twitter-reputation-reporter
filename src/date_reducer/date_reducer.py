import multiprocessing

from middleware.rabbitmq_queue import RabbitMQQueue

DATE = 0
SCORE = 1

NEGATIVE_SCORE = -1

POSITIVE = "positive"
NEGATIVE = "negative"

DATE_RECEIVE_QUEUE_NAME = "date_twits"
DATE_SEND_QUEUE_NAME = "date_processed_twits"

class DateReducer(multiprocessing.Process):
    def __init__(self, receive_rabbitmq_queue, send_rabbitmq_queue):
        multiprocessing.Process.__init__(self)
        self.receive_rabbitmq_queue = receive_rabbitmq_queue
        self.send_rabbitmq_queue = send_rabbitmq_queue
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
        print("------------------Entre al date reducer--------------------")
        self.receive_rabbitmq_queue.consume(self._callback)
        for date in self.dates:
            self.send_rabbitmq_queue.send("{},{},{}".format(date, self.dates[date][POSITIVE], self.dates[date][NEGATIVE]))
        self.send_rabbitmq_queue.send_eom()
        print("------------------Sali del date reducer--------------------")

if __name__ == '__main__':
    date_reducer_workers = int(os.environ['DATE_REDUCER_WORKERS'])
    analyzer_workers = int(os.environ['ANALYZER_WORKERS'])

    send_rabbitmq_queue =  RabbitMQQueue(DATE_SEND_QUEUE_NAME, rabbitmq_host)

    workers = []

    for i in range(date_reducer_workers):
        receive_rabbitmq_queue = RabbitMQQueue("{}{}".format(DATE_RECEIVE_QUEUE_NAME, i), rabbitmq_host, analyzer_workers)
        workers.append(DateReducer(receive_rabbitmq_queue, send_rabbitmq_queue))

    for i in range(date_reducer_workers):
        workers[i].run()

    for i in range(date_reducer_workers):
        workers[i].join()

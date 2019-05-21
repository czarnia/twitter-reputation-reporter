import multiprocessing
from dateutil.parser import parse

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.rabbitmq_queues import RabbitMQQueues

AUTHOR_ID = 1
INBOUND = 2
CREATED_AT = 3
TEXT = 4

NUM_COLUMS = 7

IS_CUSTOMER = "True"

RECEIVE_QUEUE_NAME = "preprocesed_twits"
SEND_QUEUE_NAME = "raw_twits"

class FilterParser(multiprocessing.Process):
    def __init__(self, id, rabbitmq_host, next_workers_number):
        multiprocessing.Process.__init__(self)
        self.send_queues = RabbitMQQueues(RECEIVE_QUEUE_NAME, rabbitmq_host, next_workers_number)
        self.receive_queue = RabbitMQQueue("{}{}".format(SEND_QUEUE_NAME, id), rabbitmq_host)

    def run(self):
        self.receive_queue.consume(self._callback)
        self.send_queues.send_eom()

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').rstrip().split(",")

        if (len(body_values) != NUM_COLUMS) or (body_values[INBOUND] != IS_CUSTOMER):
            return

        day = str(parse(body_values[CREATED_AT]).date())
        self.send_queues.send("{},{},{}".format(body_values[AUTHOR_ID], day, body_values[TEXT]), body_values[AUTHOR_ID])

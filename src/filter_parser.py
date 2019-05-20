import multiprocessing
from dateutil.parser import parse

from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 1
INBOUND = 2
CREATED_AT = 3
TEXT = 4

NUM_COLUMS = 7

class FilterParser(multiprocessing.Process):
    def __init__(self, next_workers_number):
        multiprocessing.Process.__init__(self)
        self.next_workers_number = next_workers_number
        self.send_queue = RabbitMQQueue("preprocesed_twits", 'rabbitmq')
        self.receive_queue = RabbitMQQueue("raw_twits", 'rabbitmq')

    def run(self):
        self.receive_queue.consume(self._callback)
        for i in range(self.next_workers_number):
            self.send_queue.send_eom()

    def _callback(self, ch, method, properties, body):
        body_values = body.decode('UTF-8').rstrip().split(",")

        if (len(body_values) != NUM_COLUMS) or (body_values[INBOUND] != "True"):
            return

        day = str(parse(body_values[CREATED_AT]).date())
        self.send_queue.send("{},{},{}".format(body_values[AUTHOR_ID], day, body_values[TEXT]))

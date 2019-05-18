import multiprocessing
from middleware.rabbitmq_queue import RabbitMQQueue

AUTHOR_ID = 1
INBOUND = 2
CREATED_AT = 3
TEXT = 4

class FilterParser(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.send_queue = RabbitMQQueue("preprocesed_twits", 'rabbitmq')
        self.receive_queue = RabbitMQQueue("raw_twits", 'rabbitmq')

    def run(self):
        self.receive_queue.consume(self._callback)

    def _callback(self, ch, method, properties, body):
        body_values = body.split(",")

        if body_values[INBOUND] != "True":
            return

        self.sender.send("%s,%s,%s".format(body_values[AUTHOR_ID], body_values[CREATED_AT], body_values[TEXT]))

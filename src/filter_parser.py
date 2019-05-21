import multiprocessing
from dateutil.parser import parse

from middleware.rabbitmq_queue import RabbitMQQueue
from middleware.rabbitmq_queues import RabbitMQQueues

AUTHOR_ID = 1
INBOUND = 2
CREATED_AT = 3
TEXT = 4

NUM_COLUMS = 7

class FilterParser(multiprocessing.Process):
    def __init__(self, id, next_workers_number):
        multiprocessing.Process.__init__(self)
        self.send_queues = RabbitMQQueues("preprocesed_twits", 'rabbitmq', next_workers_number)
        self.receive_queue = RabbitMQQueue("raw_twits{}".format(id), 'rabbitmq')

    def run(self):
        #print("-------------FILTER, EMPIEZO--------------")
        self.receive_queue.consume(self._callback)
        self.send_queues.send_eom()
        #print("-------------FILTER, TERMINO--------------")
        #print("")

    def _callback(self, ch, method, properties, body):
        #print("-------------------FILTER, RECIBO LA LINEA {}---------------------".format(body.decode('UTF-8')))
        body_values = body.decode('UTF-8').rstrip().split(",")

        if (len(body_values) != NUM_COLUMS) or (body_values[INBOUND] != "True"):
            return

        #print("-------------------FILTER, ENVIO LA LINEA {}---------------------".format(body.decode('UTF-8')))
        day = str(parse(body_values[CREATED_AT]).date())
        self.send_queues.send("{},{},{}".format(body_values[AUTHOR_ID], day, body_values[TEXT]), body_values[AUTHOR_ID])

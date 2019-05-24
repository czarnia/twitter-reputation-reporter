from .rabbitmq_queue import RabbitMQQueue

class RabbitMQQueues(object):
        def __init__(self, queue_name, rabbit_host, number_of_queues, number_of_producers = None):
            self.connection_host = rabbit_host
            self.queue = queue_name
            self.queues = [ RabbitMQQueue("{}{}".format(queue_name, i), 'rabbitmq', number_of_producers) for i in range(number_of_queues) ]

        def _hash(self, value):
            return hash(value) % len(self.queues)

        def send(self, msg, key):
            queue = self.queues[self._hash(key)]
            queue.send(msg)

        def send_eom(self):
            for queue in self.queues:
                queue.send_eom()

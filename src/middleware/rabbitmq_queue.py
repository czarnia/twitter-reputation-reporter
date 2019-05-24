import pika

MSG_EOM = "None"

class RabbitMQQueue(object):
    def __init__(self, queue_name, rabbit_host, number_of_producers = 1):
        self.connection_host = rabbit_host
        self.queue = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.connection_host, heartbeat=600,
                                       blocked_connection_timeout=300))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.tag = None

    def __exit__(self, *args):
        self.connection.close()

    def send(self, msg):
        self.channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=msg,
                              properties=pika.BasicProperties(delivery_mode = 2,))

    def consume(self, callback):
        def _callback_wrapper(ch, method, properties, body):
            if body.decode('UTF-8') == MSG_EOM:
                self.number_of_producers -= 1
                if self.number_of_producers == 0:
                    self._stop_consuming()
                return

            callback(ch,method,properties,body)

        self.tag = self.channel.basic_consume(queue=self.queue, auto_ack=True, on_message_callback=_callback_wrapper)
        self.channel.start_consuming()

    def send_eom(self):
        self.send(MSG_EOM)

    def _stop_consuming(self):
        self.channel.basic_cancel(self.tag)

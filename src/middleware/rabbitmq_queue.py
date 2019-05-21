import pika
import requests

MSG_EOM = "None"

class RabbitMQQueue(object):
    def __init__(self, queue_name, rabbit_host):
        self.connection_host = rabbit_host
        self.queue = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.connection_host, heartbeat=600,
                                       blocked_connection_timeout=300))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.tag = None
        self.number_of_current_producers = None

    def __exit__(self, *args):
        self.connection.close()

    def send(self, msg):
        self.channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=msg,
                              properties=pika.BasicProperties(delivery_mode = 2,))

    def consume(self, callback, number_of_producers = 1):
        self.number_of_current_producers = number_of_producers
        def _callback_wrapper(ch, method, properties, body):
            if body.decode('UTF-8') == MSG_EOM:
                self.number_of_current_producers -= 1
                #print("------------------RESTO UNO, TENGO {}---------------------".format(self.number_of_current_producers))
                if self.number_of_current_producers == 0:
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                    self._stop_consuming()
                    #print("")
                    #print("--------------RabbitMQQueue, MANDO STOP_CONSUMING--------------")
                return
            callback(ch,method,properties,body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.tag = self.channel.basic_consume(queue=self.queue,on_message_callback=_callback_wrapper)
        self.channel.start_consuming()

    def send_eom(self):
        self.send(MSG_EOM)

    def _stop_consuming(self):
        self.channel.basic_cancel(self.tag)

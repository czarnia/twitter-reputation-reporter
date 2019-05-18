import pika

MSG_EOM = "None"

class RabbitMQQueue(object):
    def __init__(self, queue_name, rabbit_host):
        self.connection_host = rabbit_host
        self.queue = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.connection_host))
        self.channel = self.connection.channel()
        self.tag = None

    def __exit__(self, *args):
        self.connection.close()

    def send(self, msg):
        self.channel.queue_declare(queue=self.queue, durable=True)

        self.channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=msg,
                              properties=pika.BasicProperties(delivery_mode = 2,))

    def consume(self,callback):
    	self.channel.queue_declare(queue=self.queue,durable = True)

    	def _callback_wrapper(ch, method, properties, body):
    		if body == MSG_EOM:
    			self._stop_consuming()
    		callback(ch,method,properties,body)
    		ch.basic_ack(delivery_tag=method.delivery_tag)

    	self.tag = self.channel.basic_consume(queue=self.queue,on_message_callback=_callback_wrapper)
    	self.channel.start_consuming()

    def consume(self, callback):
        self.channel.queue_declare(queue=self.queue, durable=True)
        self.tag = self.channel.basic_consume(
            queue=self.queue, on_message_callback=self._callback_wrapper(callback))
        self.channel.start_consuming()

    def send_eom(self):
        self.send(MSG_EOM)

    def _stop_consuming(self):
        self.channel.basic_cancel(self.tag)

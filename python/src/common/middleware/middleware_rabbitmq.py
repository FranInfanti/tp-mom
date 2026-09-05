import pika
import random
import string

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
    MessageMiddlewareDeleteError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.queue_name = queue_name

        # creates a conn with RabbitMQ broker
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # defines a chan that allows to communicate with RabbitMQ broker
        self.chan = self.conn.channel()

        # declare the queue cause it might not exist yet
        self.chan.queue_declare(queue_name)

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            on_message_callback(
                message=body, 
                ack=lambda: ch.basic_ack(delivery_tag=method.delivery_tag),
                nack=lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
            )

        try:
            self.chan.basic_consume(
                queue=self.queue_name,
                on_message_callback=callback,
                auto_ack=False
            )

            self.chan.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def stop_consuming(self):
        try:
            self.chan.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()

    def send(self, message):
        try:
            self.chan.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception:
            raise MessageMiddlewareMessageError()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            raise MessageMiddlewareCloseError()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass

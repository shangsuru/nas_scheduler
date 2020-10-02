import time
import config
import threading
import abc

from kombu import Connection, Producer, Consumer, Exchange, Queue, eventloop
from kombu.mixins import ConsumerMixin

from log import logger

class XCHQueues():
    exchange = Exchange('hub_exchange', type='direct')
    queues = {
        'scheduler': Queue('scheduler', exchange, routing_key='scheduler'),
        'progressor': Queue('progressor', exchange, routing_key='progressor'),
        'statsor': Queue('statsor', exchange, routing_key='statsor'),
        'timer': Queue('timer', exchange, routing_key='timer')
    }

class Payload():
    def __init__(self, timestamp, source, type, content={}):
        self.timestamp = timestamp
        self.source = source
        self.type = type
        self.content = content

class Router():
    def __init__(self):
        self.connection = Connection(config.AMQP_URI)
        self.connection.connect()

        self.producer = Producer(self.connection)

    def __del__(self):
        self.connection.release()
        self.producer.release()

    def push(self, msg, destination):
        self.producer.publish(msg,
            retry=True,
            retry_policy={
                'interval_start': 0, # First retry immediately,
                'interval_step': 2,  # then increase by 2s for every retry.
                'interval_max': 30,  # but don't exceed 30s between retries.
                'max_retries': 30,   # give up after 30 tries.
            },
            exchange=XCHQueues.exchange,
            routing_key=destination,
            serializer='pickle', 
            compression='zlib')


class Handler(threading.Thread, ConsumerMixin, metaclass=abc.ABCMeta):
    def __init__(self, connection, entity):
        threading.Thread.__init__(self, daemon=True)
        self.connection = connection
        self.entity = entity

    def __del__(self):
        logger.debug(f'[{self.entity}] exited.')

    def run(self):
        logger.debug(f'[{self.entity}] started within {threading.currentThread().getName()}')
        ConsumerMixin.run(self)

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[XCHQueues.queues[self.entity]],
                            accept=['json'],
                            callbacks=[self.__handle_message])
        consumer.qos(prefetch_count=10)
        return [consumer]

    def __handle_message(self, body, message):
        logger.debug(f'[{self.entity}] Received message {body}')
        self.process(body)
        message.ack()

    @abc.abstractmethod
    def process(self, msg):
        pass

hub = Router()

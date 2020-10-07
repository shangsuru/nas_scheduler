import time
import config
import threading
import abc

from kombu import Connection, Producer, Consumer, Exchange, Queue
from kombu.common import Broadcast
from kombu.mixins import ConsumerMixin

from log import logger

class XCHQueues():
    exchange = Exchange('hub_exchange', type='direct')
    broadcast_exchange = Exchange('hub_fanout', type='fanout', auto_delete=True)
    queues = {
        'scheduler': Queue('scheduler', exchange, routing_key='scheduler'),
        'progressor': Queue('progressor', exchange, routing_key='progressor'),
        'statsor': Queue('statsor', exchange, routing_key='statsor'),
        'timer': Queue('timer', exchange, routing_key='timer'),
        'simulator': Queue('simulator', exchange, routing_key='simulator'),
        'broadcast': Broadcast(name='broadcast', exchange=broadcast_exchange, auto_delete=True),
    }

class Payload():
    def __init__(self, timestamp=None, source=None, type=None, content={}):
        self.timestamp = timestamp
        self.source = source
        self.type = type
        self.content = content

    def __str__(self):
        return f'Payload(t={self.timestamp}, source={self.source}, type={self.type}, content={self.content})'

    def fetch_content(self, entity):
        if self.content:
            return self.content.get(entity, None)
        else:
            return None

class Router():
    def __init__(self):
        self.connection = Connection(config.AMQP_URI)
        self.connection.connect()

        self.producer = Producer(self.connection)

    def __del__(self):
        self.connection.release()
        self.producer.release()

    def push(self, body, destination):
        self.producer.publish(body,
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

    def broadcast(self, body):
        self.producer.publish(body,
            retry=True,
            retry_policy={
                'interval_start': 0, # First retry immediately,
                'interval_step': 2,  # then increase by 2s for every retry.
                'interval_max': 30,  # but don't exceed 30s between retries.
                'max_retries': 30,   # give up after 30 tries.
            },
            exchange=XCHQueues.broadcast_exchange,
            serializer='json',
            compression='zlib')


class Handler(threading.Thread, ConsumerMixin, metaclass=abc.ABCMeta):
    def __init__(self, connection, entity):
        threading.Thread.__init__(self, daemon=True)
        self.connection = connection
        self.entity = entity

    def __del__(self):
        logger.debug(f'[{self.module_name}] exited.')

    def run(self):
        logger.debug(f'[{self.module_name}] started within {threading.currentThread().getName()}')
        ConsumerMixin.run(self)

    def stop(self):
        self.should_stop = True

    def get_consumers(self, Consumer, channel):
        consumer = Consumer(queues=[XCHQueues.queues[self.entity], XCHQueues.queues['broadcast']],
                            accept=['json', 'pickle'],
                            callbacks=[self.__handle_message])
        consumer.qos(prefetch_count=10)
        return [consumer]

    def __handle_message(self, body, message):
        logger.debug(f'[{self.module_name}] Received message {str(body)}')
        if body == 'stop':
            self.stop()
        else:
            message.ack()
            self.process(body)

    @abc.abstractmethod
    def process(self, msg):
        pass

hub = Router()

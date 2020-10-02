import time
import threading

import config

from kombu.mixins import ConsumerMixin
from communication import Handler, hub
from log import logger

class Timer(Handler):
    def __init__(self):
        super().__init__(connection=hub.connection, entity='timer')
        self.clock = 1
        self.start()

    def process(self, msg):
        # assert msg['timestamp'] == self.clock
        
        # scheduler have finished its slot
        self.clock += 1
        logger.debug(f'[Timer] increment clock. New clock: {self.clock}')
        
    def get_clock(self):
        return self.clock

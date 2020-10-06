import time
import threading

import config

from communication import Handler, Payload, hub
from log import logger

class Timer(Handler):
    def __init__(self):
        super().__init__(connection=hub.connection, entity='timer')
        self.module_name = 'timer'
        self.clock = 1
        self.start()

    def process(self, msg):
        # assert msg['timestamp'] == self.clock
        
        # scheduler have finished its slot
        if msg.type == 'read':
            hub.push(Payload(self.clock, 'timer', 'update', {'time': self.clock}), msg.source)
        elif msg.type == 'reset':
            self.clock = 1
            hub.push(Payload(self.clock, 'timer', 'update', {'time': self.clock}), msg.source)
        else:
            self.clock += 1
            hub.push(Payload(self.clock, 'timer', 'update', {'time': self.clock}), 'simulator')
            logger.debug(f'[{self.module_name}] increment clock. New clock: {self.clock}')
        
    def get_clock(self):
        return self.clock

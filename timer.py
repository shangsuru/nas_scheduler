import redis
import config
from log import logger


class Timer:
    """
    Class that keeps track of the number of the timeslot where different jobs
    can be submitted.
    """

    clock = 1
    redis_connection = redis.Redis(config.REDIS_HOST_DC, config.REDIS_PORT_DC)

    @staticmethod
    def reset_clock():
        Timer.clock = 1
        return Timer.clock

    @staticmethod
    def update_clock():
        Timer.clock += 1
        logger.debug(f"increment clock. New clock: {Timer.clock}")

        # broadcast next time slot to redis listeners on channel timer
        Timer.redis_connection.publish("timer", Timer.clock)
        return Timer.clock

    @staticmethod
    def get_clock():
        return Timer.clock

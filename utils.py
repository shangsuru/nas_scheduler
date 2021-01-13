import asyncio
import functools


def list_to_str(listofstr):
    return ",".join(listofstr)


class objectview(object):
    def __init__(self, d):
        if d is None:
            self.__dict__ = {}
        else:
            self.__dict__ = d


def update_dict_keys(input, attr_map):
    new = {}
    for k, v in input.items():
        if isinstance(v, dict):
            v = update_dict_keys(v, attr_map)
        if isinstance(v, list):
            for idx, i in enumerate(v):
                if isinstance(i, dict):
                    v[idx] = update_dict_keys(v[idx], attr_map)
        new[attr_map.get(k, k)] = v
    return new


def rgetattr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split("."))


def dict_to_str(dict_obj):
    """
    Converts dict object to a string of key=values separated by commas
    """
    return ", ".join(f"{key}={val}" for (key, val) in dict_obj.items())


async def fetch_with_timeout(redis_connection, key, timeout, num_retries=100):
    """
    Used when it is required to fetch a key from redis with a timeout

    Args:
        redis_connection: connection to the redis database
        key (str): key for the value to be fetched from redis
        timeout (float): time (in ms) until a subsequent attempt to fetch the key
        num_retries (int): number of attempts to fetch the key

    Raises:
        TimeoutError: value could not be fetched with given number of retries
    """
    value = redis_connection.get(key)
    for _ in range(num_retries):
        await asyncio.sleep(timeout / 1000)
        value = redis_connection.get(key)
        if value:
            return value
    raise TimeoutError

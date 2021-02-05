import asyncio
import functools
import redis
from typing import Any, Callable, List


def list_to_str(listofstr: List[str]) -> str:
    return ",".join(listofstr)


class objectview(object):
    def __init__(self, d: dict) -> None:
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


def dict_to_str(dict_obj: dict) -> str:
    """
    Converts dict object to a string of key=values separated by commas
    """
    return ", ".join(f"{key}={val}" for (key, val) in dict_obj.items())


async def fetch_with_timeout(
    redis_connection: redis.Redis, key: str, timeout: float, num_retries: int = 100, cast: Callable[[Any], Any] = None
) -> Any:
    """
    Used when it is required to fetch a key from redis with a timeout

    Args:
        redis_connection: connection to the redis database
        key: key for the value to be fetched from redis
        timeout: time (in ms) until a subsequent attempt to fetch the key
        num_retries: number of attempts to fetch the key
        cast (func): function that casts an object to another type

    Raises:
        TimeoutError: value could not be fetched with given number of retries
    """
    value = redis_connection.get(key)
    for _ in range(num_retries):
        await asyncio.sleep(timeout / 1000)
        value = redis_connection.get(key)
        if value:
            if cast:
                return cast(value)
            return value
    raise TimeoutError

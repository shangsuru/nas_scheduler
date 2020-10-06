import functools


def list_to_str(listofstr):
    return ','.join(listofstr)


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

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def dict_to_str(dict_obj):
    """Converts dict object to a string of key=values separated by commas
    """
    return ', '.join(f'{key}={val}' for (key, val) in dict_obj.items())

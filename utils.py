import functools

def list_to_str(listofstr):
	return ','.join(listofstr)

class objectview(object):
    def __init__(self, d):
        self.__dict__ = d

def rgetattr(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)
    return functools.reduce(_getattr, [obj] + attr.split('.'))

def dict_to_str(dict_obj):
	"""Converts dict object to a string of key=values separated by commas
	"""
	return ', '.join(f'{key}={val}' for (key,val) in dict_obj.items())
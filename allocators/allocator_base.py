import abc

class ResourceAllocator(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    @abc.abstractmethod
    def allocate(self, jobs):
        pass
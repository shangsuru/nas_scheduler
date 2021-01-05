import abc


class ResourceAllocator(metaclass=abc.ABCMeta):
    def __init__(self, cluster):
        self.cluster = cluster

    @abc.abstractmethod
    def allocate(self, jobs):
        pass
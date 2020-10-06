import abc


class ResourceAllocator(metaclass=abc.ABCMeta):
    def __init__(self, cluster):
        self.cluster = cluster
        self.module_name = 'allocator'

    @abc.abstractmethod
    def allocate(self, jobs):
        pass

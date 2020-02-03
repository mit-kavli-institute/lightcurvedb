from __future__ import print_function, division
from abc import abstractmethod, ABC

class Ingestor(ABC):

    EmissionModel = None

    def __init__(self, context_kwargs={}):
        self.context = context_kwargs

    @abstractmethod
    def parse(self, descriptor):
        pass

    def preingesthook(self):
        pass

    def postingesthook(self):
        pass

    def ingest(self, file, mode='rt'):
        self.preingesthook()
        with open(file, mode) as file_in:
            for parsed_info in self.parse(file_in):
                yield self.translate(**parsed_info)
        self.postingesthook()

    def translate(self, **emission_kwargs):
        instance = self.EmissionModel(**emission_kwargs)
        return instance
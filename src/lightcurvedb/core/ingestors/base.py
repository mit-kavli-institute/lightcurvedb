from __future__ import division, print_function

from abc import ABC, abstractmethod


class Ingestor(ABC):

    EmissionModel = None

    def __init__(self, context_kwargs=None):
        self.context = context_kwargs if context_kwargs else {}

    @abstractmethod
    def parse(self, descriptor):
        pass

    def preingesthook(self):
        pass

    def postingesthook(self):
        pass

    def ingest(self, file, mode="rt"):
        self.preingesthook()
        with open(file, mode) as file_in:
            for parsed_info in self.parse(file_in):
                yield self.translate(**parsed_info)
        self.postingesthook()

    def translate(self, **emission_kwargs):
        instance = self.EmissionModel(**emission_kwargs)
        return instance


class MultiIngestor(Ingestor):
    def ingest(self, files, mode="rt"):
        self.preingesthook()
        for f in files:
            for parsed_info in self.parse(f):
                yield self.translate(**parsed_info)
        self.postingesthook()


class PyObjIngestor(Ingestor):
    @abstractmethod
    def parse(self, obj):
        pass

    def ingest(self, obj):
        self.preingesthook()
        objs = obj
        parsed = self.parse(objs)
        for parsed_info in parsed:
            yield self.translate(**parsed_info)
        self.postingesthook()

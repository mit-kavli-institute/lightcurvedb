class IngestionTelemetry:
    def __init__(self, amount, seconds, unit="unit"):
        self.amount = amount
        self.seconds = seconds
        self.unit = unit

    def __repr__(self):
        return f"{self.rate} + {self.unit}s"

    @property
    def rate(self):
        return amount / time


class LightpointIngestionParameters:
    def __init__(self):
        raise NotImplementedError

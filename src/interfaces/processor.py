from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class IProcessor(ABC):
    @abstractmethod
    def process(self) -> None:
        pass

class ITransformer(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass
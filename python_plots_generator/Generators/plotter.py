from abc import ABC, abstractmethod
import pandas as pd
import Config.constants as cts

class Plotter(ABC):
    @staticmethod
    def load_dataframe(path: str):
        return pd.read_parquet(cts.SPARK_BASE_DIR + path, engine='pyarrow')

    @abstractmethod
    def create_plot(self):
        pass

    @abstractmethod
    def save_plot(self):
        pass
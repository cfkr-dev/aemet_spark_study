from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class Plotter(ABC):
    @staticmethod
    def load_dataframe(path: Path):
        return pd.read_parquet(str(path), engine='pyarrow')

    @abstractmethod
    def create_plot(self):
        pass

    @abstractmethod
    def save_plot(self, figure):
        pass
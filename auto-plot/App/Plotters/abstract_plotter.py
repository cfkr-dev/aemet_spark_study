from abc import ABC, abstractmethod
from pathlib import Path

from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.Parquet.parquet_storage_backend import ParquetStorageBackend


class Plotter(ABC):
    @staticmethod
    def load_dataframe(path: Path, storage: Storage):
        return ParquetStorageBackend.read_parquet(path.as_posix(), storage)

    @abstractmethod
    def create_plot(self):
        pass

    @abstractmethod
    def save_plot(self, figure):
        pass

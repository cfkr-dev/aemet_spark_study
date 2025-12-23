"""Abstract base plotter utilities.

This module defines an abstract :class:`Plotter` base class that other
plotter implementations inherit from. It also exposes a small helper
``load_dataframe`` which uses the project's Parquet storage backend.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from App.Utils.Storage.Core.storage import Storage
from App.Utils.Storage.Parquet.parquet_storage_backend import ParquetStorageBackend


class Plotter(ABC):
    """Abstract base class for concrete plotter implementations.

    Concrete plotters must implement :meth:`create_plot` and
    :meth:`save_plot`.
    """

    @staticmethod
    def load_dataframe(path: Path, storage: Storage):
        """Load a dataframe from a Parquet file using the configured storage.

        :param path: Resolved path to the parquet file.
        :type path: pathlib.Path
        :param storage: Storage backend instance used to read the file.
        :type storage: App.Utils.Storage.Core.storage.Storage
        :returns: A pandas.DataFrame read from the Parquet file.
        """
        return ParquetStorageBackend.read_parquet(path.as_posix(), storage)

    @abstractmethod
    def create_plot(self):
        """Create and return the plot figure.

        Concrete implementations must return a figure object compatible
        with the export utilities (e.g. a Plotly figure).
        """
        pass

    @abstractmethod
    def save_plot(self, figure):
        """Persist the provided figure using the configured storage backend.

        Implementations should return the resolved destination path or
        ``None`` when ``figure`` is ``None``.

        :param figure: Plot object created by :meth:`create_plot`.
        """
        pass

"""Helper to read Parquet datasets via a storage backend.

The reader expects the storage backend to return a temporary directory
containing the parquet files; pandas with the pyarrow engine is used to
read the dataset.
"""

import shutil
from pathlib import Path

import pandas as pd

from App.Utils.Storage.Core.storage import Storage


class ParquetStorageBackend:
    """Small adapter that reads parquet datasets using the Storage API."""

    @staticmethod
    def read_parquet(path: str, storage: Storage):
        """Read a parquet dataset referenced by ``path`` using ``storage``.

        :param path: Logical path (may refer to a directory containing parquet files).
        :type path: str
        :param storage: Storage backend used to obtain the temporary local directory.
        :type storage: App.Utils.Storage.Core.storage.Storage
        :returns: A :class:`pandas.DataFrame` containing the parquet dataset.
        :rtype: pandas.DataFrame
        """
        tmp_dir: Path = storage.read_directory(path)
        df = pd.read_parquet(str(tmp_dir), engine='pyarrow')
        shutil.rmtree(tmp_dir)
        return df

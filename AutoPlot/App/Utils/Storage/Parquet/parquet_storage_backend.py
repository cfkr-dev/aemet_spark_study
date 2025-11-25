import shutil
from pathlib import Path

import pandas as pd

from App.Utils.Storage.Core.storage import Storage


class ParquetStorageBackend:

    @staticmethod
    def read_parquet(path: str, storage: Storage):
        tmp_dir: Path = storage.read_directory(path)
        df = pd.read_parquet(str(tmp_dir), engine='pyarrow')
        shutil.rmtree(tmp_dir)
        return df

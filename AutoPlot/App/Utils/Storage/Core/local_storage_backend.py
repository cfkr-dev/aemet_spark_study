import shutil
import tempfile
from pathlib import Path

from App.Utils.Storage.Core.storage_exceptions import StorageException, LocalFileNotFoundException


class LocalStorageBackend:
    TEMP_DIR = Path(tempfile.gettempdir())

    @staticmethod
    def exists(path: str):
        p = Path(path)
        return p.exists()

    @staticmethod
    def read(path: str):
        src = Path(path)
        if not src.exists():
            raise LocalFileNotFoundException(path)

        dst = LocalStorageBackend.TEMP_DIR / src.name

        try:
            shutil.copy(src, dst)
            return dst
        except Exception as ex:
            raise StorageException(f"Failed to read local file: {path}", ex)

    @staticmethod
    def read_directory(path: str):
        src_dir = Path(path)
        if not src_dir.exists() or not src_dir.is_dir():
            raise LocalFileNotFoundException(path)

        target_dir = tempfile.mkdtemp()

        for file in src_dir.iterdir():
            if file.is_file():
                shutil.copy(file, Path(target_dir) / file.name)

        return target_dir

    @staticmethod
    def write(path: str, local_path: Path):
        dst = Path(path)
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(local_path, dst)
        except Exception as ex:
            raise StorageException(f"Failed to write local file: {path}", ex)

import logging
from pathlib import Path
import shutil
from ._file_store import FileStore

logger = logging.getLogger(__name__)


class LocalFileStore(FileStore):
    def is_file(self, remote_path):
        return Path(remote_path).is_file()

    def is_dir(self, remote_path):
        return Path(remote_path).is_dir()

    def ls(self, remote_path, recursive=False):
        path = Path(remote_path)
        if path.is_file():
            return [remote_path]
        if path.is_dir():
            if recursive:
                z = path.glob('**/*')
            else:
                z = path.glob('*')
            return [str(v) if v.is_file() else str(v) + '/' for v in z]
        return []

    def read_bytes(self, remote_file):
        return Path(remote_file).read_bytes()

    def write_bytes(self, data, remote_file, overwrite=False):
        f = Path(remote_file)
        if not overwrite and f.is_file():
            raise FileExistsError(remote_file)
        Path(f.parent).mkdir(exist_ok=True)
        f.write_bytes(data)

    def read_text(self, remote_file):
        return Path(remote_file).read_text()

    def write_text(self, data, remote_file, overwrite=False):
        f = Path(remote_file)
        if not overwrite and f.is_file():
            raise FileExistsError(remote_file)
        Path(f.parent).mkdir(exist_ok=True)
        f.write_text(data)

    def download(self, remote_file, local_file, overwrite=False):
        if remote_file == local_file:
            raise shutil.SameFileError(local_file)
        f = Path(local_file)
        if f.is_file():
            if overwrite:
                f.unlink()
            else:
                raise FileExistsError(local_file)
        shutil.copyfile(remote_file, local_file)

    def upload(self, local_file, remote_file, overwrite=False):
        self.download(local_file, remote_file, overwrite=overwrite)

    def download_dir(self, remote_dir, local_dir, overwrite=False, verbose=True):
        if local_dir == remote_dir:
            raise shutil.SameFileError(local_dir)
        if Path(local_dir).is_dir():
            if overwrite:
                # TODO: overwrite file-wise or clear the whole directory?
                shutil.rmtree(local_dir)
            else:
                raise FileExistsError(local_dir)
        if verbose:
            logger.info("copying content of directory '%s' into '%s'",
                        remote_dir, local_dir)
        shutil.copytree(remote_dir, local_dir)

    def upload_dir(self, local_dir, remote_dir, overwrite=False, verbose=True):
        self.download_dir(local_dir, remote_dir,
                          overwrite=overwrite, verbose=verbose)

    def rm(self, remote_file, missing_ok=False):
        f = Path(remote_file)
        if f.is_file():
            f.unlink()
        elif f.is_dir():
            raise Exception(
                f"'{remote_file}' is a directory; please use `rm_dir` to remove")
        elif missing_ok:
            return
        else:
            raise FileNotFoundError(remote_file)

    def rm_dir(self, remote_dir, missing_ok=False, verbose=True):
        f = Path(remote_dir)
        if f.is_dir():
            if verbose:
                logger.info('deleting directory %s', remote_dir)
            shutil.rmtree(remote_dir)
        elif f.is_file():
            raise Exception(
                f"'{remote_dir}' is a file; please use `rm` to remove")
        elif missing_ok:
            return
        else:
            raise FileNotFoundError(remote_dir)

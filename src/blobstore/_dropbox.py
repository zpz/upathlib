import logging
import json
import os
import os.path
import pickle
from contextlib import contextmanager
from pathlib import Path
from typing import List, Any

from ..path import join_path
from ._local import LocalFileStore
from ._file_store import FileStore
from ._datetime import make_timestamp, TIMESTAMP_FILE

logger = logging.getLogger(__name__)


class Dropbox:
    def __init__(self,
                 remote_store: FileStore,
                 remote_root_dir: str,
                 local_root_dir: str):
        self.remote_root_dir = remote_root_dir
        self.local_root_dir = local_root_dir
        self.pwd = '/'
        self.pwd_history = []
        self.remote_store = remote_store
        self.local_store = LocalFileStore()

    def _abs_path(self, path: str, *paths):
        # The return starts with '/', which indicates
        # `self.remote_root_dir` and `self.local_root_dir`.
        # 'absolute' is relative to that root.
        if paths:
            path = os.path.join(path, *paths)
        if not path.startswith('/'):
            path = join_path(self.pwd, path)
        return path

    def _remote_real_path(self, path: str, *paths):
        path = self._abs_path(path, *paths).lstrip('/')
        return os.path.join(self.remote_root_dir, path)

    def _local_real_path(self, path: str, *paths):
        path = self._abs_path(path, *paths).lstrip('/')
        return os.path.join(self.local_root_dir, path)

    @contextmanager
    def cd(self, path: str, *paths):
        # This command can be nested, e.g.
        #
        #    with dropbox.cd('abc') as box1:
        #        with box1.cd('de') as box2:
        #            ...
        pwd = self._abs_path(path, *paths)
        if not pwd.endswith('/'):
            pwd += '/'
        self.pwd_history.append(self.pwd)
        self.pwd = pwd
        try:
            return self
        finally:
            self.pwd = self.pwd_history.pop()

    def remote_is_file(self, path: str) -> bool:
        f = self._remote_real_path(path)
        if f.endswith('/'):
            return False
        return self.remote_store.is_file(f)

    def remote_is_dir(self, path: str) -> bool:
        f = self._remote_real_path(path)
        if not f.endswith('/'):
            f += '/'
        return self.remote_store.is_dir(f)

    def local_is_file(self, path: str) -> bool:
        f = self._local_real_path(path)
        if f.endswith('/'):
            return False
        return Path(f).is_file()

    def local_is_dir(self, path: str) -> bool:
        f = self._local_real_path(path)
        if not f.endswith('/'):
            f += '/'
        return Path(f).is_dir()

    def remote_ls(self, path: str = './', recursive: bool = False) -> List[str]:
        f = self._remote_real_path(path)
        zz = self.remote_store.ls(f, recursive=recursive)
        n = len(self.remote_root_dir)
        return sorted(v[n:] for v in zz)
        # TODO: get absolute '/' right.

    def local_ls(self, path: str = './', recursive: bool = False) -> List[str]:
        f = self._local_real_path(path)
        zz = self.local_store.ls(f, recursive=recursive)
        n = len(self.local_root_dir)
        return sorted(v[n:] for v in zz)
        # TODO: get absolute '/' right.

    def remote_read_bytes(self, file_path: str) -> bytes:
        f = self._remote_real_path(file_path)
        return self.remote_store.read_bytes(f)

    def remote_read_text(self, file_path) -> str:
        f = self._remote_real_path(file_path)
        return self.remote_store.read_text(f)

    def remote_read_json(self, file_path) -> Any:
        return json.loads(self.remote_read_text(file_path))

    def remote_read_pickle(self, file_path) -> Any:
        return pickle.loads(self.remote_read_bytes(file_path))

    def remote_write_bytes(self, data: bytes, file_path: str, overwrite: bool = False) -> None:
        f = self._remote_real_path(file_path)
        self.remote_store.write_bytes(data, f, overwrite=overwrite)

    def remote_write_text(self, text, file_path, overwrite=False):
        f = self._remote_real_path(file_path)
        self.remote_store.write_text(text, f, overwrite=overwrite)

    def remote_write_json(self, x, file_path, overwrite=False):
        self.remote_write_text(json.dumps(x), file_path, overwrite=overwrite)

    def remote_write_pickle(self, x, file_path, overwrite=False):
        self.remote_write_bytes(pickle.dumps(
            x), file_path, overwrite=overwrite)

    def local_read_bytes(self, file_path: str) -> bytes:
        f = self._local_real_path(file_path)
        return self.local_store.read_bytes(f)

    def local_read_text(self, file_path) -> str:
        f = self._local_real_path(file_path)
        return self.local_store.read_text(f)

    def local_read_json(self, file_path) -> Any:
        return json.loads(self.local_read_text(file_path))

    def local_read_pickle(self, file_path) -> Any:
        return pickle.loads(self.local_read_bytes(file_path))

    def local_write_bytes(self, data: bytes, file_path: str, overwrite: bool = False) -> None:
        f = self._local_real_path(file_path)
        self.local_store.write_bytes(data, f, overwrite=overwrite)

    def local_write_text(self, text, file_path, overwrite=False):
        f = self._local_real_path(file_path)
        self.local_store.write_text(text, f, overwrite=overwrite)

    def local_write_json(self, x, file_path, overwrite=False):
        self.local_write_text(json.dumps(x), file_path, overwrite=overwrite)

    def local_write_pickle(self, x, file_path, overwrite=False):
        self.local_write_bytes(pickle.dumps(
            x), file_path, overwrite=overwrite)

    def download(self, file_path: str, overwrite: bool = False):
        assert not file_path.endswith('/')
        remote_file = self._remote_real_path(file_path)
        local_file = self._local_real_path(file_path)
        self.remote_store.download(
            remote_file, local_file, overwrite=overwrite)

    def upload(self, file_path: str, overwrite: bool = False):
        assert not file_path.endswith('/')
        local_file = self._local_real_path(file_path)
        remote_file = self._remote_real_path(file_path)
        self.remote_store.upload(
            local_file, remote_file, overwrite=overwrite)

    def download_dir(self,
                     dir_path: str,
                     *,
                     overwrite: bool = False,
                     clear_local_dir: bool = False,
                     verbose: bool = True):
        if (self.remote_has_timestamp(dir_path)
                and self.local_has_timestamp(dir_path)):
            remote_ts = self.remote_read_timestamp(dir_path)
            local_ts = self.local_read_timestamp(dir_path)
            if local_ts >= remote_ts:
                return 0

        if clear_local_dir:
            self.local_store.rm_dir(dir_path, missing_ok=True, verbose=verbose)
        self.remote_store.download_dir(
            self._remote_real_path(dir_path),
            self._local_real_path(dir_path),
            overwrite=overwrite,
            verbose=verbose)

    def upload_dir(self,
                   dir_path: str,
                   *,
                   overwrite: bool = False,
                   clear_remote_dir: bool = False,
                   verbose: bool = True):
        if (self.remote_has_timestamp(dir_path)
                and self.local_has_timestamp(dir_path)):
            remote_ts = self.remote_read_timestamp(dir_path)
            local_ts = self.local_read_timestamp(dir_path)
            if remote_ts >= local_ts:
                return 0

        if clear_remote_dir:
            self.remote_store.rm_dir(
                dir_path, missing_ok=True, verbose=verbose)
        self.remote_store.upload_dir(
            self._local_real_path(dir_path),
            self._remote_real_path(dir_path),
            overwrite=overwrite,
            verbose=verbose,
        )

    def remote_rm(self, file_path, missing_ok: bool = False):
        f = self._remote_real_path(file_path)
        self.remote_store.rm(f, missing_ok=missing_ok)

    def remote_rm_dir(self, file_path, missing_ok: bool = False, verbose: bool = True):
        f = self._remote_real_path(file_path)
        self.remote_store.rm_dir(f, missing_ok=missing_ok, verbose=verbose)

    def local_rm(self, file_path, missing_ok: bool = False):
        f = self._remote_real_path(file_path)
        self.local_store.rm(f, missing_ok=missing_ok)

    def local_rm_dir(self, file_path, missing_ok: bool = False, verbose: bool = True):
        f = self._remote_real_path(file_path)
        self.local_store.rm_dir(f, missing_ok=missing_ok, verbose=verbose)

    def remote_has_timestamp(self, *paths) -> bool:
        return self.remote_is_file(*paths, TIMESTAMP_FILE)

    def remote_write_timestamp(self, *paths) -> None:
        self.remote_write_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def remote_read_timestamp(self, *paths) -> str:
        return self.remote_read_text(*paths, TIMESTAMP_FILE)

    def local_has_timestamp(self, *paths) -> bool:
        return self.local_is_file(*paths, TIMESTAMP_FILE)

    def local_write_timestamp(self, *paths) -> None:
        self.local_write_text(make_timestamp(), *paths, TIMESTAMP_FILE)

    def local_read_timestamp(self, *paths) -> str:
        return self.local_read_text(*paths, TIMESTAMP_FILE)

    def remote_make_dir(self, path: str, *paths):
        if not self.remote_is_dir(path, *paths):
            self.remote_write_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def local_make_dir(self, path: str, *paths):
        if not self.local_is_dir(path, *paths):
            self.local_write_text(
                make_timestamp(), path, *paths, TIMESTAMP_FILE)

    def remote_clear(self, verbose: bool = True):
        self.remote_rm_dir('./', verbose=verbose)

    def local_clear(self, verbose: bool = True):
        self.local_rm_dir('./', verbose=verbose)

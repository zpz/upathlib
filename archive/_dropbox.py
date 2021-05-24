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
from ._upath import FileStore
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


# def _get_cp_dest(abs_source_file: str, abs_dest_path: str) -> str:
#     # Get the destination file path as if we do
#     #    cp abs_source_file abs_dest_path
#     if abs_dest_path.endswith('/'):
#         assert not abs_source_file.endswith('/')
#         return os.path.join(abs_dest_path, os.path.basename(abs_source_file))
#     return abs_dest_path


# class Store(ABC):
#     '''
#     This class creates/operates a space in a file storage system for
#     file upload, download, deletion, duplication, movement, renaming,
#     reading, etc.

#     For convenience, this space will be referred to as a 'store' in this class.
#     Think of it as a 'directory', or 'box', or 'container'.
#     The basic unit of storage in a store is a 'file', or 'blob'.

#     Within the store, we use POSIX style of path representations to locate blobs.
#     In particular,

#     - root is '/'
#     - segment separator is '/'

#     Note 'root' is the root **inside** the store.
#     It does not have to be located at the root of the file system;
#     rather, it can be a 'directory', then '/' within this class refers to
#     this directory.

#     The location of this root in the file system (outside of the store)
#     is specified by the `home` parameter of `__init__`, and can be queried
#     by the property `home`.

#     Operations within the store can not go beyond this store root.
#     For example, if `home` is '/home/user/writings/`, then we ar free
#     to navigate through the subdirectories of `/home/user/writtings/`,
#     but can not access `/home/user/`.

#     In the store, 'directories' are *virtual*, meaning we do not need to think about
#     directories as concrete things and 'create' or 'delete' them.
#     They are transparent to the user.
#     If there is a blob with path `/ab/cd/ef.txt`, then we say 'directory'
#     `/ab/cd/` exists. If there is no blob with path like `/ab/cd/*`, then
#     directory `/ab/cd/` does not exist.

#     We use this naming convention:

#         *file or *file_path:  a file (i.e. blob)
#         *dir or *dir_path: a (virtual) directory
#         *path:  either file or directory

#     Any in-store path ending with '/' is considered a *directory*,
#     and otherwise a *file*.

#     In-store paths can always be written as either relative or absolute.
#     A relative path is relative to the 'current working directory', which is
#     returned by `self.pwd` (which is always absolute).
#     '''


#     def abspath(self, path: str) -> str:
#         '''
#         This gives the 'absolute' path within the store,
#         in other words, the return value starts with '/'
#         and that refers to the root within the store,
#         i.e. `self.home`.

#         `path` may be given as relative to `self.pwd`,
#         or as an absolute path within the store (which would be returned
#         w/o change).
#         '''
#         return join_path(self.pwd, path)

#     def _cp(self, abs_source_file: str, abs_dest_file: str) -> None:
#         '''
#         Duplicate `abs_source_file` as `abs_dest_file`, which is known to be non-existent.

#         This is not a `abstractmethod`. It's not needed by other methods of this class.
#         If a subclass does not find this functionality needed, it does not need to implement it.
#         '''
#         raise NotImplementedError

#     def cp(self, source_file: str, dest_path: str, forced: bool=False) -> None:
#         '''
#         Copy within the store.
#         '''
#         abs_source_file = self.abspath(source_file)
#         _assert_is_file(abs_source_file)
#         abs_dest_file = _get_cp_dest(abs_source_file, self.abspath(dest_path))
#         if self._exists_file(abs_dest_file):
#             if forced:
#                 self._rm(abs_dest_file)
#             else:
#                 raise RuntimeError(f"file '{self.realpath(abs_dest_file)}' already exists")
#         self._cp(abs_source_file, abs_dest_file)

#     def put_dir(self, local_abs_dir: str, dir_path: str='.', clear_dir_first: bool=False, forced: bool=False, include_hidden: bool=False) -> None:
#         _assert_is_abs(local_abs_dir)
#         _assert_is_dir(local_abs_dir)
#         dir_path = self.abspath(dir_path)
#         _assert_is_dir(dir_path)
#         if self.exists(dir_path):
#             if clear_dir_first:
#                 for z in self._ls_dir(dir_path):
#                     self.rm(z)

#         def listit(dir_path):
#             for z in os.listdir(dir_path):
#                 if (not include_hidden) and z.startswith('.'):
#                     continue
#                 zz = os.path.join(dir_path, z)
#                 if os.path.isdir(zz):
#                     return listit(zz)
#                 if os.path.isfile(zz):
#                     yield zz

#         len_prefix = len(local_abs_dir)
#         pwd = self.pwd
#         self.cd(dir_path)
#         for z in listit(local_abs_dir):
#             zz = z[len_prefix: ]
#             self.put(z, z[len_prefix : ], forced=forced)
#         self.cd(pwd)

#     def get_dir(self, dir_path: str, local_abs_dir: str, clear_dir_first: bool=False, forced: bool=False) -> None:
#         dir_path = self.abspath(dir_path)
#         _assert_is_dir(dir_path)
#         _assert_is_abs(local_abs_dir)
#         _assert_is_dir(local_abs_dir)
#         if os.path.isdir(local_abs_dir):
#             if clear_dir_first:
#                 shutil.rmtree(local_abs_dir)
#         else:
#             os.makedirs(local_abs_dir, exist_ok=True)

#         len_prefix = len(dir_path)
#         for z in self._ls_dir(dir_path):
#             zz = os.path.join(local_abs_dir, z)
#             self.get(z, zz, forced=forced)

from abc import ABC, abstractmethod
from typing import List


class FileStore(ABC):
    @abstractmethod
    def is_file(self, remote_path: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def is_dir(self, remote_path: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def ls(self, remote_path: str, recursive: bool = False) -> List[str]:
        # `remote_path` is either a file or a directory.
        # If `recursive` is `True`, the order of the returned elements
        # does not matter.
        raise NotImplementedError

    @abstractmethod
    def read_bytes(self, remote_file: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def write_bytes(self, data: bytes, remote_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def read_text(self, remote_file: str) -> str:
        raise NotImplementedError

    @abstractmethod
    def write_text(self, data: str, remote_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def download(self, remote_file: str, local_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def upload(self, local_file: str, remote_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def download_dir(self, remote_dir: str, local_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError

    @abstractmethod
    def upload_dir(self, local_dir: str, remote_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError

    @abstractmethod
    def rm(self, remote_file: str, missing_ok: bool = False) -> None:
        raise NotImplementedError

    @abstractmethod
    def rm_dir(self, remote_dir: str, missing_ok: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError

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

#     def __init__(self, home: str='/'):
#         '''
#         `home` is the location of the store in the file system.
#         Usually this is like a 'directory'.
#         '''
#         if not home.endswith('/'):
#             home += '/'
#         self._home = home
#         self._pwd = '/'

#     @property
#     def home(self) -> str:
#         '''
#         Do not define setter and deleter for this property.
#         This is a read-only attribute.
#         '''
#         return self._home

#     @property
#     def pwd(self) -> str:
#         '''
#         Get the 'present working directory'.
#         This is an 'absolute' path within the store, hence the leading '/' refers to `self.home`.

#         Do not define setter and deleter for this property.
#         This is a read-only attribute.

#         To set `pwd`, use the `cd` method.
#         '''
#         return self._pwd

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

#     def realpath(self, path: str) -> str:
#         '''
#         This returns the 'real' absolute path in the file system.
#         This is the concatenation of `self.home` and the path within the store.
#         '''
#         return os.path.join(self.home, self.abspath(path)[1:])

#     def cd(self, path: str=None) -> 'Store':
#         '''
#         Change the 'present working directory'.
#         '''
#         if path is None:
#             self._pwd = '/'
#         else:
#             z = self.abspath(path)
#             if not z.endswith('/'):
#                 z += '/'
#             self._pwd = z
#         return self

#     @abstractmethod
#     def _exists_file(self, abs_path: str) -> bool:
#         raise NotImplementedError

#     def exists(self, path: str) -> bool:
#         abs_path = self.abspath(path)
#         if _is_file(abs_path):
#             return self._exists_file(abs_path)
#         else:
#             return len(self._ls_dir(abs_path)) > 0

#     @abstractmethod
#     def _ls_dir(self, abs_path: str, recursive: bool=False) -> List[str]:
#         '''
#         List items below the directory `abs_path`.
#         The returned paths are relative to `abs_path`.
#         Subdirectories have a trailing `/`.
#         '''
#         raise NotImplementedError

#     def ls(self, path: str='.', recursive: bool=False) -> List[str]:
#         abs_path = self.abspath(path)
#         if _is_file(abs_path):
#             if self._exists_file(abs_path):
#                 return [path]
#             else:
#                 return []

#         z = self._ls_dir(abs_path, recursive)
#         if z:
#             z = [os.path.join(path, v) for v in z]

#         return z

#     @abstractmethod
#     def _rm(self, abs_path: str) -> None:
#         '''
#         Remove single file `abs_path` that is known to exist.
#         '''
#         raise NotImplementedError

#     def rm(self, path: str, forced: bool=False) -> None:
#         abs_path = self.abspath(path)
#         _assert_is_file(abs_path)
#         if self._exists_file(abs_path):
#             self._rm(abs_path)
#         else:
#             if not forced:
#                 raise RuntimeError(f"file '{self.realpath(abs_path)}' does not exist")

#     def _stat(self, abs_path: str):
#         '''
#         Return info about the file which is known to exist.
#         '''
#         raise NotImplementedError

#     def stat(self, file_path: str):
#         '''
#         Return info about the file, such as size, datetime of creation,
#         full path.
#         '''
#         abs_file = self.abspath(file_path)
#         if not self._exists_file(abs_file):
#             raise RuntimeError(f"file '{file_path}' does not exist")
#         return self._stat(abs_file)

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

#     def _mv(self, abs_source_file: str, abs_dest_file: str) -> None:
#         '''
#         Rename `abs_source_file` to `abs_dest_file`, which is known to be non-existent.

#         This is not a `abstractmethod`. It's not needed by other methods of this class.
#         If a subclass does not find this functionality needed, it does not need to implement it.
#         '''
#         raise NotImplementedError

#     def mv(self, source_file: str, dest_path: str, forced: bool=False) -> None:
#         '''
#         Move within the store.
#         '''
#         abs_source_file = self.abspath(source_file)
#         _assert_is_file(abs_source_file)
#         abs_dest_file = _get_cp_dest(abs_source_file, self.abspath(dest_path))
#         if self._exists_file(abs_dest_file):
#             if forced:
#                 self._rm(abs_dest_file)
#             else:
#                 raise RuntimeError(f"file '{self.realpath(abs_dest_file)}' already exists")
#         self._mv(abs_source_file, abs_dest_file)

#     @abstractmethod
#     def _put(self, local_abs_file: str, abs_file: str) -> None:
#         '''
#         Copy `local_abs_file`, which is known to be existent,
#         into the store as `abs_file`, which is known to be non-existent.
#         '''
#         raise NotImplementedError

#     def put(self, local_abs_file: str, path: str='./', forced: bool=False) -> None:
#         abs_dest_file = _get_cp_dest(local_abs_file, self.abspath(path))
#         if self._exists_file(abs_dest_file):
#             if forced:
#                 self._rm(abs_dest_file)
#             else:
#                 raise RuntimeError(f"file '{self.realpath(abs_dest_file)}' already exists")
#         if not os.path.isfile(local_abs_file):
#             raise RuntimeError(f"file '{local_abs_file}' does not exist")
#         self._put(local_abs_file, abs_dest_file)

#     @abstractmethod
#     def _get(self, abs_file: str, local_abs_file: str) -> None:
#         '''
#         Download blob `abs_file`, which is known to be existent,
#         as `local_abs_file`, which is know to be non-existent.
#         '''
#         raise NotImplementedError

#     def get(self, file_path: str, local_abs_path: str, forced: bool=False) -> None:
#         abs_file = self.abspath(file_path)
#         _assert_is_file(abs_file)
#         if not self._exists_file(abs_file):
#             raise RuntimeError(f"file '{self.realpath(abs_file)}' does not exist")
#         local_abs_dest_file = _get_cp_dest(abs_file, local_abs_path)
#         if os.path.isfile(local_abs_dest_file):
#             if forced:
#                 os.remove(local_abs_dest_file)
#             else:
#                 raise RuntimeError(f"file '{local_abs_dest_file}' already exists")
#         else:
#             os.makedirs(os.path.dirname(local_abs_dest_file), exist_ok=True)

#         self._get(abs_file, local_abs_dest_file)

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

#     def open(self, file_path: str, mode: str='rt'):
#         raise NotImplementedError

#     def put_text(self, text: str, file_path: str, forced: bool=False) -> None:
#         if forced:
#             self.open(file_path, 'w').write(text)
#         else:
#             self.open(file_path, 'x').write(text)

#     def get_text(self, file_path: str) -> str:
#         return self.open(file_path).read()

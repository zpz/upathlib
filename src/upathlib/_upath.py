import os
import os.path
from pathlib import PurePosixPath
from typing import List, Union, Tuple, Generator


class PureUpath(os.PathLike):
    def __init__(self, *parts: Union[str, os.PathLike]):
        if parts:
            path_s = os.path.normpath(os.path.join(*parts))
        else:
            path_s = '/'
        assert not path_s.startswith('.')
        if not path_s.startswith('/'):
            path_s = '/' + path_s
        self._path = PurePosixPath(path_s)
        # The path is always "absolute" starting with '/'.

    def __fspath__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        try:
            return self.__hash__
        except AttributeError:
            self._hash = hash(self.__str__())
            return self._hash

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({str(self._path)})'

    def __str__(self) -> str:
        return str(self._path)

    def __truediv__(self, key: str) -> 'PureUpath':
        return self.joinpath(key)

    def _compare_(self, op, other):
        if not (other.__class__ is self.__class__):
            return NotImplemented
        return op(str(other))

    def __eq__(self, other) -> bool:
        return self._compare_(self.__str__().__eq__, other)

    def __lt__(self, other) -> bool:
        return self._compare_(self.__str__().__lt__, other)

    def __le__(self, other) -> bool:
        return self._compare_(self.__str__().__le__, other)

    def __gt__(self, other) -> bool:
        return self._compare_(self.__str__().__gt__, other)

    def __ge__(self, other) -> bool:
        return self._compare_(self.__str__().__ge__, other)

    def joinpath(self, *parts: str) -> 'PureUpath':
        return self.__class__(self._path.joinpath(*parts))

    def match(self, path_pattern: str) -> bool:
        return self._path.match(path_pattern)

    @property
    def name(self) -> str:
        return self._path.name

    @property
    def parent(self) -> 'PureUpath':
        return self.__class__(str(self._path.parent))

    @property
    def parts(self) -> Tuple[str, ...]:
        return self._path.parts

    def relative_to(self, other: Union[str, 'PureUpath']) -> str:
        if isinstance(other, str):
            other = self.__class__(other)
        return self._path.relative_to(other._path)

    def is_relative_to(self, other: Union[str, 'PureUpath']) -> bool:
        try:
            self.relative_to(other)
            return True
        except ValueError:
            return False

    @property
    def root(self) -> str:
        return '/'

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def suffix(self) -> str:
        return self._path.suffix

    @property
    def suffixes(self) -> List[str]:
        return self._path.suffixes

    def with_name(self, name: str) -> 'PureUpath':
        return self.__class__(str(self._path.with_name(name)))

    def with_stem(self, stem: str) -> 'PureUpath':
        # Available in Python 3.9+.
        return self.__class__(str(self._path.with_stem(stem)))

    def with_suffix(self, suffix: str) -> 'PureUpath':
        return self.__class__(str(self._path.with_suffix(suffix)))


class Upath(PureUpath):
    homesep = '//'

    def __init__(self, home: str, *parts: str):
        # TODO: `parts` can contain `PureUpath` objects.
        self._home = home or ''
        super().__init__(*parts)

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._home}, {super().__repr__()})'

    def __str__(self) -> str:
        return self._home + self.homesep + super().__str__()

    def __truediv__(self, key: str) -> 'Upath':
        return self.__class__(self._home, self._path // key)

    def _compare_(self, op, other):
        if not (other.__class__ is self.__class__):
            return NotImplemented
        if self._home != other._home:
            return NotImplemented
        return op(str(other))

    def cd(self, relpath: str) -> 'self':
        assert self.is_dir()
        p = os.path.normpath(str(self._path.joinpath(relpath)))
        self._path = self._path.__class__(p)
        return self

    def exists(self) -> bool:
        raise NotImplementedError

    def glob(self, pattern: str) -> Generator['Upath']:
        # Implemented when `is_dir()` returns `True.
        raise NotImplementedError

    def rglob(self, pattern: str) -> Generator['Upath']:
        raise NotImplementedError

    @property
    def home(self) -> 'Upath':
        return self.__class__(self._home)

    def is_dir(self) -> bool:
        raise NotImplementedError

    def is_file(self) -> bool:
        raise NotImplementedError

    def iterdir(self) -> Generator['Upath']:
        # Implemented when `is_dir()` returns `True.
        raise NotImplementedError

    def joinpath(self, *parts: str) -> 'Upath':
        return self.__class__(self._home, self._path.joinpath(*parts))

    def mkdir(parents: bool = False, exist_ok: bool = False):
        raise NotImplementedError

    def open(self, mode: str = 'r'):
        raise NotImplementedError

    @property
    def parent(self) -> 'Upath':
        return self.__class__(self._home, self._path.parent)

    def read_bytes(self) -> bytes:
        raise NotImplementedError

    def read_text(self, encoding: str = None, errors: str = None):
        # Refer to https://docs.python.org/3/library/functions.html#open
        raise NotImplementedError

    def relative_to(self, other: Union[str, 'Upath']) -> str:
        if isinstance(other, str):
            other = self.__class__(self._home, other)
        else:
            if not (other.__class__ is self.__class__):
                raise ValueError('`other` must be either a string or an object of class {}'.format(
                    self.__class__.__name__
                ))
            if other._home != self._home:
                return False
        return self._path.relative_to(other._path)

    def rename(self, target: Union[str, 'Upath']) -> 'Upath':
        raise NotImplementedError

    def replace(self, target: Union[str, 'Upath']) -> 'Upath':
        raise NotImplementedError

    def rm(self, missing_ok: bool = False) -> None:
        raise NotImplementedError

    def rmdir(self) -> None:
        raise NotImplementedError

    def samefile(self, other_path: Union[str, 'Upath']) -> bool:
        raise NotImplementedError

    def stat(self):
        raise NotImplementedError

    def with_name(self, name: str) -> 'Upath':
        return self.__class__(self._home, super().with_name(name))

    def with_stem(self, stem: str) -> 'Upath':
        return self.__class__(self._home, super().with_stem(stem))

    def with_suffix(self, suffix: str) -> 'Upath':
        return self.__class__(self._home, super().with_suffix(suffix))

    def write_bytes(self, data: bytes) -> int:
        raise NotImplementedError

    def write_text(self, data: str, encoding=None, errors=None) -> int:
        raise NotImplementedError


class Dropbox:
    def __init__(self, remote: Upath, local: Upath = None):
        pass

    def download(self, remote_file: str, local_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    def upload(self, local_file: str, remote_file: str, overwrite: bool = False) -> None:
        raise NotImplementedError

    def download_dir(self, remote_dir: str, local_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
        raise NotImplementedError

    def upload_dir(self, local_dir: str, remote_dir: str, overwrite: bool = False, verbose: bool = True) -> None:
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


# class LocalUPath(Upath):
#     def is_file(self, remote_path):
#         return Path(remote_path).is_file()

#     def is_dir(self, remote_path):
#         return Path(remote_path).is_dir()

#     def ls(self, remote_path, recursive=False):
#         path = Path(remote_path)
#         if path.is_file():
#             return [remote_path]
#         if path.is_dir():
#             if recursive:
#                 z = path.glob('**/*')
#             else:
#                 z = path.glob('*')
#             return [str(v) if v.is_file() else str(v) + '/' for v in z]
#         return []

#     def read_bytes(self, remote_file):
#         return Path(remote_file).read_bytes()

#     def write_bytes(self, data, remote_file, overwrite=False):
#         f = Path(remote_file)
#         if not overwrite and f.is_file():
#             raise FileExistsError(remote_file)
#         Path(f.parent).mkdir(exist_ok=True)
#         f.write_bytes(data)

#     def read_text(self, remote_file):
#         return Path(remote_file).read_text()

#     def write_text(self, data, remote_file, overwrite=False):
#         f = Path(remote_file)
#         if not overwrite and f.is_file():
#             raise FileExistsError(remote_file)
#         Path(f.parent).mkdir(exist_ok=True)
#         f.write_text(data)

#     def download(self, remote_file, local_file, overwrite=False):
#         if remote_file == local_file:
#             raise shutil.SameFileError(local_file)
#         f = Path(local_file)
#         if f.is_file():
#             if overwrite:
#                 f.unlink()
#             else:
#                 raise FileExistsError(local_file)
#         shutil.copyfile(remote_file, local_file)

#     def upload(self, local_file, remote_file, overwrite=False):
#         self.download(local_file, remote_file, overwrite=overwrite)

#     def download_dir(self, remote_dir, local_dir, overwrite=False, verbose=True):
#         if local_dir == remote_dir:
#             raise shutil.SameFileError(local_dir)
#         if Path(local_dir).is_dir():
#             if overwrite:
#                 # TODO: overwrite file-wise or clear the whole directory?
#                 shutil.rmtree(local_dir)
#             else:
#                 raise FileExistsError(local_dir)
#         if verbose:
#             logger.info("copying content of directory '%s' into '%s'",
#                         remote_dir, local_dir)
#         shutil.copytree(remote_dir, local_dir)

#     def upload_dir(self, local_dir, remote_dir, overwrite=False, verbose=True):
#         self.download_dir(local_dir, remote_dir,
#                           overwrite=overwrite, verbose=verbose)

#     def rm(self, remote_file, missing_ok=False):
#         f = Path(remote_file)
#         if f.is_file():
#             f.unlink()
#         elif f.is_dir():
#             raise Exception(
#                 f"'{remote_file}' is a directory; please use `rm_dir` to remove")
#         elif missing_ok:
#             return
#         else:
#             raise FileNotFoundError(remote_file)

#     def rm_dir(self, remote_dir, missing_ok=False, verbose=True):
#         f = Path(remote_dir)
#         if f.is_dir():
#             if verbose:
#                 logger.info('deleting directory %s', remote_dir)
#             shutil.rmtree(remote_dir)
#         elif f.is_file():
#             raise Exception(
#                 f"'{remote_dir}' is a file; please use `rm` to remove")
#         elif missing_ok:
#             return
#         else:
#             raise FileNotFoundError(remote_dir)

import abc
import logging
import pathlib
from typing import Union

from ._upath import Upath
from ._local import LocalUpath


logger = logging.getLogger(__name__)


class BlobUpath(Upath):  # pylint: disable=abstract-method
    def __init__(self, *parts: str, **kwargs):
        super().__init__(*parts, **kwargs)
        self._as_dir = None
        if self._path == '/':
            self._as_dir = True
        else:
            if parts:
                if parts[-1].endswith('/'):
                    self._as_dir = True

    @abc.abstractmethod
    def _blob_exists(self) -> bool:
        # Unless `self.path` is '/', the path
        # does not end with '/'. This function determines
        # whether a blob with this name exists.
        # If it does, it is equivalent to a *file*.
        # Note the difference between `_blob_exists`
        # and `exists`.
        raise NotImplementedError

    def clear(self):
        n = 0
        for p in self.iterdir():
            p.rm()
            n += 1
        if n > 0:
            self._as_dir = True

    def download(self,
                 target: Union[str, pathlib.Path, LocalUpath],
                 *,
                 exist_action: str = None) -> int:
        if isinstance(target, str):
            target = pathlib.Path(target)
        if isinstance(target, pathlib.Path):
            target = LocalUpath(str(target.absolute()))
        else:
            assert isinstance(target, LocalUpath)
        return self.copy_to(target, exist_action=exist_action)

    def exists(self):
        if self._blob_exists():
            return True
        try:
            next(self.iterdir())
            return True
        except StopIteration:
            return False

    def is_dir(self):
        '''In a typical blob store, there is no such concept as a
        "directory". Here we emulate the situation in a local file
        system. If there are blobs named like

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have blobs named like

            /ab/cd/ef/

        (I don't know whether the blob store offerings allow
        such blob names.)

        Consequently, `is_dir` is almost equivalent
        to "having stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        However, we provide two ways to emulate an "empty dir".
        The first way is a call to `mkdir`. The second way is
        to include a trailing '/' in the name, as in

            BlobUpath('ab', 'cd', 'efg/')
            blob_upath / 'xy/'

        Both ways mark the path as a dir in the remaining life
        of the BlobUpath object. The mark is not persisted anywhere
        outside of the object. Given the subtlety involved, this
        feature is not highlighted for now.
        '''
        if self._as_dir:
            return True
        try:
            next(self.iterdir())
            return True
        except StopIteration:
            if self._blob_exists():
                return False
            return None

    def is_file(self):
        if self._blob_exists():
            return True
        return None

    def iterdir(self):
        # For efficiency reasons, this does not first check that
        # `self` is a dir, and raise NotADirectoryError if it isn't.
        # This could change later, to be aligned with the behavior of
        # `LocalUpath` as well as `pathlib`.
        p0 = self._path  # this could be '/'.
        if not p0.endswith('/'):
            p0 += '/'
        np0 = len(p0)
        subdirs = set()
        for p in self.riterdir():
            tail = p._path[np0:]
            if '/' in tail:
                tail = tail[: tail.find('/')]
            if tail not in subdirs:
                yield self / tail
                subdirs.add(tail)

    def mkdir(self, *, exist_ok=False):
        if self.is_dir():
            if exist_ok or self.is_empty_dir():
                return self
            raise FileExistsError(self)
        else:
            # Make sure that a path name can't be both a file
            # and a directory.
            p = self
            while p._path != '/':
                if p.is_file():
                    raise FileExistsError(p)
                p = p.parent

            self._as_dir = True
            # There is no need to "create a directory"
            # in a blob store. Just go ahead creating
            # blobs under the "directory".
            return self

    def rmdir(self):
        try:
            next(self.riterdir())
            raise FileExistsError(self)
        except StopIteration:
            self._as_dir = False

    def upload(self,
               source: Union[str, pathlib.Path, LocalUpath],
               *,
               exist_action: str = None) -> int:
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath(str(source.absolute()))
        else:
            assert isinstance(source, LocalUpath)
        return self.copy_from(source, exist_action=exist_action)

    async def a_download(self, *args, **kwargs):
        return await self._a_do(self.download, *args, **kwargs)

    async def a_upload(self, *args, **kwargs):
        return await self._a_do(self.upload, *args, **kwargs)

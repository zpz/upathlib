import concurrent.futures
import logging
import pathlib
from typing import Union

from ._upath import Upath
from ._local import LocalUpath


logger = logging.getLogger(__name__)


class BlobUpath(Upath):  # pylint: disable=abstract-method
    def download(self,
                 target: Union[str, pathlib.Path, LocalUpath],
                 *,
                 concurrency: int = None,
                 exist_action: str = None) -> int:
        if isinstance(target, str):
            target = pathlib.Path(target)
        if isinstance(target, pathlib.Path):
            target = LocalUpath(str(target.absolute()))
        else:
            assert isinstance(target, LocalUpath)
        return self.copy_to(target,
                            concurrency=concurrency,
                            exist_action=exist_action)

    def exists(self):
        if self.isfile():
            return True
        return self.isdir()

    def isdir(self):
        '''In a typical blob store, there is no such concept as a
        "directory". Here we emulate the concept in a local file
        system. If there is a blob named like

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have a trailing `/` in a blob's name, like

            /ab/cd/ef/

        (I don't know whether the blob stores allow
        such blob names.)

        Consequently, `isdir` is equivalent
        to "having stuff in the dir". There is no such thing as
        an "empty directory" in blob stores.
        '''
        try:
            next(self.riterdir())
            return True
        except StopIteration:
            return False

    def iterdir(self):
        p0 = self._path  # this could be '/'.
        if not p0.endswith('/'):
            p0 += '/'
        np0 = len(p0)
        subdirs = set()
        for p in self.riterdir():
            tail = p._path[np0:]
            if tail.startswith('/'):
                raise Exception(f"malformed blob name '{p._path}'")
            if '/' in tail:
                tail = tail[: tail.find('/')]
            if tail not in subdirs:
                yield self / tail
                subdirs.add(tail)

    def rmdir(self, *, missing_ok: bool = False, concurrency: int = None) -> int:
        if concurrency is None:
            concurrency = 4
        else:
            0 <= concurrency <= 16

        if concurrency <= 1:
            n = 0
            for p in self.riterdir():
                p.rmfile()
                n += 1
            if n == 0 and not missing_ok:
                raise FileNotFoundError(self)
            return n

        pool = concurrent.futures.ThreadPoolExecutor(concurrency)
        tasks = []
        for p in self.riterdir():
            tasks.append(pool.submit(
                p.rmfile,
                missing_ok=False,
            ))
        n = 0
        for f in concurrent.futures.as_completed(tasks):
            n += f.result()
        if n == 0 and not missing_ok:
            raise FileNotFoundError(self)
        return n

    def upload(self,
               source: Union[str, pathlib.Path, LocalUpath],
               *,
               concurrency: int = None,
               exist_action: str = None) -> int:
        if isinstance(source, str):
            source = pathlib.Path(source)
        if isinstance(source, pathlib.Path):
            source = LocalUpath(str(source.absolute()))
        else:
            assert isinstance(source, LocalUpath)
        return self.copy_from(source,
                              concurrency=concurrency,
                              exist_action=exist_action)

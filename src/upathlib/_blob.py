import asyncio
import concurrent.futures
import pathlib
from typing import Union

from ._upath import Upath
from ._local import LocalUpath


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
        return self.export_to(target,
                              concurrency=concurrency,
                              exist_action=exist_action)

    def is_dir(self):
        '''In a typical blob store, there is no such concept as a
        "directory". Here we emulate the concept in a local file
        system. If there is a blob named like

            /ab/cd/ef/g.txt

        we say there exists directory "/ab/cd/ef".
        We should never have a trailing `/` in a blob's name, like

            /ab/cd/ef/

        (I don't know whether the blob stores allow
        such blob names.)

        Consequently, `is_dir` is equivalent
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

    def rename(self, target: str, *, overwrite: bool = False):
        target = self / target
        if target == self:
            return self
        if self.is_file():

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
        return self.import_from(source,
                                concurrency=concurrency,
                                exist_action=exist_action)

    async def a_download(self,
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
        return await self.a_export_to(
            target, concurrency=concurrency, exist_action=exist_action)

    async def a_remove_dir(self, *, missing_ok: bool = False, concurrency: int = None) -> int:
        if concurrency is None:
            concurrency = 4
        else:
            0 <= concurrency <= 16

        if concurrency <= 1:
            n = 0
            async for p in self.a_riterdir():
                n += await p.a_remove_file(missing_ok=False)
            if n == 0 and not missing_ok:
                raise FileNotFoundError(self)
            return n

        async def _remove_file(path, sem):
            async with sem:
                return await path.a_remove_file(missing_ok=False)

        sema = asyncio.Semaphore(concurrency)
        tasks = []
        async for p in self.a_riterdir():
            tasks.append(_remove_file(p, sema))
        nn = await asyncio.gather(*tasks)
        n = sum(nn)
        if n == 0 and not missing_ok:
            raise FileNotFoundError(self)
        return n

    async def a_upload(self,
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
        return await self.a_import_from(
            source, concurrency=concurrency, exist_action=exist_action)

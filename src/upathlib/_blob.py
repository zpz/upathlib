import pathlib
from ._upath import Upath, make_a_method
from ._local import LocalUpath


def _resolve_local_path(p):
    if isinstance(p, str):
        p = pathlib.Path(p)
    if isinstance(p, pathlib.Path):
        p = LocalUpath(str(p.absolute()))
    else:
        assert isinstance(p, LocalUpath)
    return p


class BlobUpath(Upath):  # pylint: disable=abstract-method
    @property
    def _blob_name(self) -> str:
        return self._path.lstrip('/')

    def download_dir(self, target, **kwargs) -> int:
        target_ = _resolve_local_path(target)
        return self.export_dir(target_, **kwargs)

    def download_file(self, target, **kwargs) -> int:
        target_ = _resolve_local_path(target)
        return self.export_file(target_, **kwargs)

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

    def upload_dir(self, source, **kwargs) -> int:
        s = _resolve_local_path(source)
        return self.import_dir(s, **kwargs)

    def upload_file(self, source, **kwargs) -> int:
        s = _resolve_local_path(source)
        return self.import_file(s, **kwargs)


for m in ('download_dir', 'download_file',
          'upload_dir', 'upload_file',
          ):
    setattr(BlobUpath, f'a_{m}', make_a_method(m))

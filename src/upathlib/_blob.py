from ._upath import Upath


class BlobUpath(Upath):  # pylint: disable=abstract-method
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

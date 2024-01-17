from __future__ import annotations

import base64
import logging
import multiprocessing
import pickle
import threading
from collections.abc import Iterable, Iterator, Sized
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TypeVar

from ._upath import PathType, Upath

logger = logging.getLogger(__name__)


@contextmanager
def lock_to_use(file: Upath, timeout=300):
    f = file.with_suffix(file.suffix + ".lock")
    with f.lock(timeout=timeout):
        yield file


def encode(x) -> str:
    """
    Encode an pickle-able object to a string w/o space and special characters,
    safe to be passed via network.
    """
    return base64.standard_b64encode(pickle.dumps(x)).decode()


def decode(y: str):
    """
    Convert the output of ``encode`` back to the original Python object.
    """
    return pickle.loads(base64.standard_b64decode(y.encode()))


def make_version(tag: str = None):
    """
    Make a version string based on current UTC time in this format::

        '20240113-073825-tag'

    where `'-tag'` is omitted if ``tag`` is falsy.
    """
    version = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    if tag:
        tag = tag.strip(" _-")
        # TODO: this does not strip all possible combinations.
        if tag:
            version = version + "-" + tag
    return version


Element = TypeVar("Element")


class Multiplexer(Iterable[Element], Sized):
    """
    Multiplexer is used to distribute data elements to multiple "workers" so that
    each element is obtained by exactly one worker.

    Typically, the data element is small in size but each requires significant time to process
    by the worker. The data elements are "hyper parameters".

    The usage consists of two main parts:

    1. In "coordinator" code, call :meth:`create_read_session` to start a new "session".
    Different sessions (at the same time or otherwise) are independent consumers of the data.

       Typically, this dataset, which is small and easy to create, is consumed only once.
       In this case, the coordinator code typically calls :meth:`new` to create a new Multiplexer,
       then calls :meth:`create_read_session` on it, and then manages to send the ID to workers.

    2. In "worker" code, use the ID that was returned by :meth:`create_read_session` to instantiate
    a Multiplexer and iterate over it. In so doing, multiple workers will obtain the data elements
    collectively, i.e., each element is obtained by exactly one worker.
    """

    @classmethod
    def new(
        cls,
        data: Iterable[Element],
        path: PathType,
        *,
        tag: str = None,
    ):
        """
        Parameters
        ----------
        data
            The data elements that need to be distributed. The elements should be pickle-able.
        path
            A directory where the data and any supporting info will be saved.
            The directory can be existent or non-existent.
            A sub-directory will be created under ``path`` to storage data and info about
            this particular multiplexer. The name of the subdirectory is a datetime string.
            ``tag`` is appended to the sub-directory name to be more informative, if so desired.

            If ``path`` is in the cloud, then the workers can be on multiple machines, and in multiple threads
            or processes on each machine.
            If ``path`` is on the local disk, then the workers are in threads or processes on the same machine.

            However, there are no strong reasons to use this facility on a local machine.

            Usually this class is used to distribute data to a cluster of machines, hence
            this path points to a location in a cloud storage that is supported by ``upathlib``.
        """
        from upathlib import resolve_path

        data = list(data)
        path = resolve_path(path) / make_version(tag)
        assert len(data) > 0
        (path / "data.pickle").write_pickle(data)
        mux_id = encode((path, None))
        return cls(mux_id)

    def __init__(
        self,
        mux_id: str,
        worker_id: str | None = None,
        timeout: int | float = 300,
    ):
        """
        Create a ``Multiplexer`` object and use it to distribute the data elements that have been
        stored by :meth:`new`.

        Parameters
        ----------
        mux_id
            The value that is returned by :meth:`create_read_session`.
        worker_id
            A string representing the current worker (i.e. this instance).
            If missing, a default is constructed based on thread name and process name.
        """
        self.path, self._session_id = decode(mux_id)
        self._worker_id = worker_id
        self._data = None
        self._timeout = timeout

    @property
    def data(self) -> list[Element]:
        """
        Return the data elements stored in this ``Multiplexer``.
        """
        if self._data is None:
            self._data = (self.path / "data.pickle").read_pickle()
        return self._data

    @property
    def worker_id(self) -> str:
        if not self._worker_id:
            self._worker_id = "{} {}".format(
                multiprocessing.current_process().name,
                threading.current_thread().name,
            )
        return self._worker_id

    def __getstate__(self):
        raise TypeError(f"Can't pickle {self.__class__.__name__} object")

    def __len__(self) -> int:
        """
        Return the number of data elements stored in this Multiplexer.
        """
        return len(self.data)

    def _mux_info_file(self, session_id: str) -> Upath:
        return self.path / ".mux" / session_id / "info.json"

    def create_read_session(self) -> str:
        """
        Let's say there is a "coordinator" and some "workers"; these are programs running in
        threads, processes, or distributed machines. The coordinator creates a new ``Multiplexer``
        and calls this method to start a "session" to read (i.e. iterate over) the elements
        in this Multiplexer::

            mux = Multiplexer.new(range(1000), '/tmp/abc/mux/')
            mux_id = mux.create_read_session()

        The ``mux_id`` is then provided to the workers, which will create ``Multiplexer`` instances
        pointing to the same dataset and participating in the reading session that has just been started::

            mux = Multiplexer(mux_id)
            for x in mux:
                ...

        The data that was provided to :meth:`new` is
        split between the workers so that each data element will be obtained by exactly one worker.

        The returned value (the "mux ID") encodes info about the location ("path") of the data storage as well as
        the newly created read session. All workers that use the same ID participate in the same read session, i.e.
        the data elements will be split between them. There can be multiple, independent read sessions going on at the same time.

        This call does not make the current Multiplexer object a participant in the read session just created.
        One has to use the returned value to create a new ``Multiplexer`` object to participate
        in the said read session. If the current object is already participating in a read session (an "old" session),
        making this call on the object does not change its role as a participant in the old session.
        This call merely creates a new read session but does not modify the current object.

        As a rule of thumb, an object created by ``Multiplexer.new(data, ...)`` is not a participant of any read session
        (even after :meth:`create_read_session` is called on it subsequently). On the other hand, an object
        created by ``Multiplexer(mux_id, ...)`` is participating in the read session that is identified by ``mux_id``.
        """
        session_id = datetime.now(timezone.utc).isoformat()
        self._mux_info_file(session_id).write_json(
            {
                "total": len(self.data),
                "next": 0,
                "time": datetime.utcnow().isoformat(),
            },
            overwrite=False,
        )
        return encode((self.path, session_id))

    def __iter__(self) -> Iterator[Element]:
        """
        Iterates over the data contained in the ``Multiplexer``.
        """
        assert self._session_id
        worker_id = self._worker_id
        timeout = self._timeout
        finfo = self._mux_info_file(self._session_id)
        while True:
            with lock_to_use(finfo, timeout=timeout) as ff:
                # In concurrent use cases, I've observed
                # `upathlib.LockAcquireError` raised here.
                # User may want to do retry here.
                ss = ff.read_json()
                # In concurrent use cases, I've observed
                # `FileNotFoundError` here. User may want
                # to do retry here.

                n = ss["next"]
                if n == ss["total"]:
                    return
                ff.write_json(
                    {
                        "next": n + 1,
                        "worker_id": worker_id,
                        "time": datetime.utcnow().isoformat(),
                        "total": ss["total"],
                    },
                    overwrite=True,
                )
            yield self.data[n]

    def stat(self, mux_id: str = None) -> dict:
        """
        Return status info of an ongoing read session.

        This is often called in the "coordinator" code on the object
        that has had its :meth:`create_read_session` called.
        ``mux_id`` is the return of :meth:`create_read_session`.
        If ``mux_id`` is ``None``, then this method is about the read session
        in which the current object is participating.
        """
        if mux_id:
            return self.__class__(mux_id).stat()
        assert self._session_id
        return self._mux_info_file(self._session_id).read_json()

    def done(self, mux_id: str = None) -> bool:
        """
        Return whether the data iteration is finished.

        This is often called in the "coordinator" code on the object
        that has had its :meth:`create_read_session` called.
        ``mux_id`` is the return of :meth:`create_read_session`.
        If ``mux_id`` is ``None``, then this method is about the read session
        in which the current object is participating.
        """
        ss = self.stat(mux_id)
        return ss["next"] == ss["total"]

    def destroy(self) -> None:
        """
        Delete all the data stored by this ``Multiplexer``, hence reclaiming the storage space.
        """
        self._data = []
        self.path.rmrf()

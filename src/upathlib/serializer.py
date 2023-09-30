import abc
import gc
import json
import pickle
import threading
import zlib
from typing import TypeVar

import zstandard

# zstandard has good compression ratio and also quite fast.
# It is very "balanced".
# lz4 has lower compression ratio than zstandard but is much faster.
#
# See:
#   https://gregoryszorc.com/blog/2017/03/07/better-compression-with-zstandard/
#   https://stackoverflow.com/questions/67537111/how-do-i-decide-between-lz4-and-snappy-compression
#   https://gist.github.com/oldcai/7230548

T = TypeVar("T")


MEGABYTE = 1048576  # 1024 * 1024
ZLIB_LEVEL = 3  # official default is 6
ZSTD_LEVEL = 3  # official default is 3
LZ4_LEVEL = (
    0  # official default is 0; high-compression value is 3, much slower at compressing
)
PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


def _loads(func, data, **kwargs):
    if len(data) < MEGABYTE:
        return func(data, **kwargs)
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    try:
        return func(data, **kwargs)
    finally:
        if isgc:
            gc.enable()


class TextSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T, **kwargs) -> str:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: str, **kwargs) -> T:
        raise NotImplementedError


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs)

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y, **kwargs)


class ByteSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T, **kwargs) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: bytes, **kwargs) -> T:
        raise NotImplementedError


class PickleSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, *, protocol=None):
        return pickle.dumps(x, protocol=protocol or PICKLE_PROTOCOL)

    @classmethod
    def deserialize(cls, y):
        return _loads(pickle.loads, y)


class ZPickleSerializer(PickleSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZLIB_LEVEL, protocol=None):
        y = super().serialize(x, protocol=protocol)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = zlib.decompress(y)
        return super().deserialize(y)


class _MyLocal(threading.local):
    # See doc string in ``cpython / Lib / _threading_local.py``.

    def __init__(self):
        self.compressor: dict[tuple[int, int], zstandard.ZstdCompressor] = {}
        self.decompressor: zstandard.ZstdDecompressor = None


class ZstdPickleSerializer(PickleSerializer):
    _cache = _MyLocal()

    # See doc on `ZstdCompressor` and `ZstdDecompressor` in
    # (github python-zstandard) `zstandard / backend_cffi.py`.

    # The `ZstdCompressor` and `ZstdDecompressor` objects can't be pickled.
    # If there are issues related to forking, check out ``os.register_at_fork``.

    @classmethod
    def serialize(cls, x, *, level=ZSTD_LEVEL, protocol=None, threads=0):
        '''
        Parameters
        ----------
        threads
            Number of threads to use to compress data concurrently. When set,
            compression operations are performed on multiple threads. The default
            value (0) disables multi-threaded compression. A value of ``-1`` means
            to set the number of threads to the number of detected logical CPUs.
        '''
        y = super().serialize(x, protocol=protocol)
        c = cls._cache.compressor.get((level, threads))
        if c is None:
            c = zstandard.ZstdCompressor(level=level, threads=threads)
            cls._cache.compressor[(level, threads)] = c
        return c.compress(y)

    @classmethod
    def deserialize(cls, y):
        if cls._cache.decompressor is None:
            cls._cache.decompressor = zstandard.ZstdDecompressor()
        y = cls._cache.decompressor.decompress(y)
        return super().deserialize(y)


try:
    # To use this, just install the Python package `lz4`.
    import lz4.frame
except ImportError:
    pass
else:

    class Lz4PickleSerializer(PickleSerializer):
        @classmethod
        def serialize(cls, x, *, level=LZ4_LEVEL, protocol=None):
            y = super().serialize(x, protocol=protocol)
            return lz4.frame.compress(y, compression_level=level)

        @classmethod
        def deserialize(cls, y):
            y = lz4.frame.decompress(y)
            return super().deserialize(y)

import abc
import gc
import json
import pickle
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


def z_compress(x: bytes, level=ZLIB_LEVEL) -> bytes:
    return zlib.compress(x, level=level)


def z_decompress(x: bytes) -> bytes:
    return zlib.decompress(x)


class ZPickleSerializer(PickleSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZLIB_LEVEL, protocol=None):
        y = super().serialize(x, protocol=protocol)
        return z_compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = z_decompress(y)
        return super().deserialize(y)


def zstd_compress(x: bytes, level=ZSTD_LEVEL) -> bytes:
    return zstandard.compress(x, level=level)


def zstd_decompress(x: bytes) -> bytes:
    return zstandard.decompress(x)


class ZstdPickleSerializer(PickleSerializer):
    _level: int = None
    _threads: int = None
    _compressor: zstandard.ZstdCompressor = None
    _decompressor: zstandard.ZstdDecompressor = None
    # See doc on `ZstdCompressor` and `ZstdDecompressor` in
    # (github python-zstandard) `zstandard / backend_cffi.py`.

    # The `ZstdCompressor` and `ZstdDecompressor` objects can't be pickled.
    # This there are issues related to forking, check out ``os.register_at_fork``.

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
        if cls._compressor is None or level != cls._level or threads != cls._threads:
            cls._compressor = zstandard.ZstdCompressor(level=level, threads=threads)
        return cls._compressor.compress(y)

    @classmethod
    def deserialize(cls, y):
        if cls._decompressor is None:
            cls._decompressor = zstandard.ZstdDecompressor()
        y = cls._decompressor.decompress(y)
        return super().deserialize(y)


try:
    import lz4.frame
except ImportError:
    pass
else:

    def lz4_compress(x: bytes, level=LZ4_LEVEL) -> bytes:
        return lz4.frame.compress(x, compression_level=level)

    def lz4_decompress(x: bytes) -> bytes:
        return lz4.frame.decompress(x)

    class Lz4PickleSerializer(PickleSerializer):
        @classmethod
        def serialize(cls, x, *, level=LZ4_LEVEL, protocol=None):
            y = super().serialize(x, protocol=protocol)
            return lz4_compress(y, level=level)

        @classmethod
        def deserialize(cls, y):
            y = lz4_decompress(y)
            return super().deserialize(y)

import abc
import gc
import json
import pickle
import zlib
from typing import TypeVar

# zstandard has good compression ratio and also quite fast.
# It is very "balanced".
# lz4 has lower compression ratio than zstandard but is much faster.
#
# See:
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


try:
    import zstandard
except ImportError:
    pass
else:

    class ZstdPickleSerializer(PickleSerializer):
        @classmethod
        def serialize(cls, x, *, level=ZSTD_LEVEL, protocol=None):
            y = super().serialize(x, protocol=protocol)
            return zstandard.compress(y, level=level)

        @classmethod
        def deserialize(cls, y):
            y = zstandard.decompress(y)
            return super().deserialize(y)


try:
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

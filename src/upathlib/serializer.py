import abc
import gc
import json
import pickle
import zlib
from typing import TypeVar


T = TypeVar('T')

PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


MEGABYTE = 1048576  # 1024 * 1024


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


class ByteSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T, **kwargs) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: bytes, **kwargs) -> T:
        raise NotImplementedError


class TextSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T, **kwargs) -> str:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: str, **kwargs) -> T:
        raise NotImplementedError


class PickleSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, *, protocol=None):
        return pickle.dumps(x, protocol=protocol)

    @classmethod
    def deserialize(cls, y):
        return _loads(pickle.loads, y)


class CompressedPickleSerializer(PickleSerializer):
    @classmethod
    def serialize(cls, x, *, level=3, protocol=None):
        y = super().serialize(x, protocol=protocol)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        z = zlib.decompress(y)
        return super().deserialize(z)


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs)

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y, **kwargs)


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        y = json.dumps(x, **kwargs)
        return y.encode()

    @classmethod
    def deserialize(cls, y, **kwargs):
        z = y.decode()
        return _loads(json.loads, z, **kwargs)


class CompressedJsonSerializer(JsonByteSerializer):
    @classmethod
    def serialize(cls, x, *, level=3, **kwargs):
        y = super().serialize(x, **kwargs)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y, **kwargs):
        z = zlib.decompress(y)
        return super().deserialize(z, **kwargs)


try:
    import orjson
except ImportError:
    pass
else:

    class OrjsonSerializer(ByteSerializer):
        @classmethod
        def serialize(cls, x, **kwargs):
            return orjson.dumps(x, **kwargs)  # pylint: disable=no-member

        @classmethod
        def deserialize(cls, y):
            return _loads(orjson.loads, y)  # pylint: disable=no-member

    class CompressedOrjsonSerializer(ByteSerializer):
        @classmethod
        def serialize(cls, x, *, level=3, **kwargs):
            y = orjson.dumps(x, **kwargs)  # pylint: disable=no-member
            return zlib.compress(y, level=level)

        @classmethod
        def deserialize(cls, y):
            z = zlib.decompress(y)
            return _loads(orjson.loads, z)  # pylint: disable=no-member

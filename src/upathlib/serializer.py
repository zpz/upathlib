import abc
import gc
import json
import pickle
import zlib
from typing import TypeVar


T = TypeVar('T')

PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


MEGABYTE = 1048576  # 1024 * 1024


def _loads(func, data):
    if len(data) < MEGABYTE:
        return func(data)
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    try:
        return func(data)
    finally:
        if isgc:
            gc.enable()



class ByteSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T) -> bytes:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: bytes) -> T:
        raise NotImplementedError


class TextSerializer(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def serialize(cls, x: T) -> str:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, y: str) -> T:
        raise NotImplementedError


class PickleSerializer(ByteSerializer):
    PROTOCOL = PICKLE_PROTOCOL

    @classmethod
    def serialize(cls, x):
        return pickle.dumps(x, protocol=cls.PROTOCOL)

    @classmethod
    def deserialize(cls, y):
        return _loads(pickle.loads, y)


class CompressedPickleSerializer(ByteSerializer):
    PROTOCOL = PICKLE_PROTOCOL

    @classmethod
    def serialize(cls, x):
        y = pickle.dumps(x, protocol=cls.PROTOCOL)
        return zlib.compress(y, level=3)

    @classmethod
    def deserialize(cls, y):
        z = zlib.decompress(y)
        return _loads(pickle.loads, z)


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        y = json.dumps(x)
        return y.encode()

    @classmethod
    def deserialize(cls, y):
        z = y.decode()
        return _loads(json.loads, z)


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x):
        return json.dumps(x)

    @classmethod
    def deserialize(cls, y):
        return _loads(json.loads, y)


try:
    import orjson
except ImportError:
    pass
else:

    ORJSON_OPT = orjson.OPT_SERIALIZE_NUMPY  # pylint: disable=no-member
    # Although this is supported, when data contains numpy,
    # you probably should serialize it by pickle, because
    # pickle would be much faster for numpy, and
    # deserialize JSON will not get back numpy arrays.

    class OrjsonSerializer(ByteSerializer):
        OPTION = ORJSON_OPT

        @classmethod
        def serialize(cls, x):
            return orjson.dumps(x, option=cls.OPTION)  # pylint: disable=no-member

        @classmethod
        def deserialize(cls, y):
            return _loads(orjson.loads, y)

    class CompressedOrjsonSerializer(ByteSerializer):
        OPTION = ORJSON_OPT

        @classmethod
        def serialize(cls, x):
            y = orjson.dumps(x, option=cls.OPTION)  # pylint: disable=no-member
            return zlib.compress(y, level=3)

        @classmethod
        def deserialize(cls, y):
            z = zlib.decompress(y)
            return _loads(orjson.loads, z)  # pylint: disable=no-member

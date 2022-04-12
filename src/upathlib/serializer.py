import abc
import gc
import json
import pickle
import zlib
from contextlib import contextmanager
from typing import TypeVar

import orjson


T = TypeVar('T')

ORJSON_OPT = orjson.OPT_SERIALIZE_NUMPY
# Although this is supported, when data contains numpy,
# you probably should serialize it by pickle, because
# deserialize JSON will not get back numpy arrays.

PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


@contextmanager
def no_gc():
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    yield
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
        if len(y) > 10_000:
            with no_gc():
                return pickle.loads(y)
        else:
            return pickle.loads(y)


class CompressedPickleSerializer(ByteSerializer):
    PROTOCOL = PICKLE_PROTOCOL

    @classmethod
    def serialize(cls, x):
        y = pickle.dumps(x, protocol=cls.PROTOCOL)
        return zlib.compress(y, level=3)

    @classmethod
    def deserialize(cls, y):
        z = zlib.decompress(y)
        if len(y) > 10_000:
            with no_gc():
                return pickle.loads(z)
        else:
            return pickle.loads(z)


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        y = json.dumps(x)
        return y.encode()

    @classmethod
    def deserialize(cls, y):
        z = y.decode()
        if len(y) > 10_000:
            with no_gc():
                return json.loads(z)
        else:
            return json.loads(z)


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x):
        return json.dumps(x)

    @classmethod
    def deserialize(cls, y):
        if len(y) > 10_000:
            with no_gc():
                return json.loads(y)
        else:
            return json.loads(y)


class OrjsonSerializer(ByteSerializer):
    OPTION = ORJSON_OPT

    @classmethod
    def serialize(cls, x):
        return orjson.dumps(x, option=cls.OPTION)  # pylint: disable=no-member

    @classmethod
    def deserialize(cls, y):
        if len(y) > 10_000:
            with no_gc():
                return orjson.loads(y)  # pylint: disable=no-member
        else:
            return orjson.loads(y)  # pylint: disable=no-member


class CompressedOrjsonSerializer(ByteSerializer):
    OPTION = ORJSON_OPT

    @classmethod
    def serialize(cls, x):
        y = orjson.dumps(x, option=cls.OPTION)  # pylint: disable=no-member
        return zlib.compress(y, level=3)

    @classmethod
    def deserialize(cls, y):
        z = zlib.decompress(y)
        if len(y) > 10_000:
            with no_gc():
                return orjson.loads(z)  # pylint: disable=no-member
        else:
            return orjson.loads(z)  # pylint: disable=no-member

import abc
import gc
import json
import pickle
import zlib
from contextlib import contextmanager
from typing import TypeVar

import orjson


T = TypeVar('T')


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
    @classmethod
    def serialize(cls, x):
        return pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return pickle.loads(y)


class CompressedPickleSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        y = pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)
        return zlib.compress(y, level=3)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            z = zlib.decompress(y)
            return pickle.loads(z)


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        y = json.dumps(x)
        return y.encode()

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            z = y.decode()
            return json.loads(z)


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x):
        return json.dumps(x)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return json.loads(y)


class OrjsonSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        return orjson.dumps(x)  # pylint: disable=no-member

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return orjson.loads(y)  # pylint: disable=no-member


class CompressedOrjsonSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        y = orjson.dumps(x)  # pylint: disable=no-member
        return zlib.compress(y, level=3)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            z = zlib.decompress(y)
            return orjson.loads(z)  # pylint: disable=no-member

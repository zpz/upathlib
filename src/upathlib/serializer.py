import abc
import gc
import json
import pickle
import zlib
from contextlib import contextmanager
from typing import TypeVar

import orjson  # type: ignore


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
        return zlib.compress(
            pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL),
            level=3)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return pickle.loads(zlib.decompress(y))


class JsonByteSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        return json.dumps(x).encode()

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return json.loads(y.decode())


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
        return orjson.dumps(x)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return orjson.loads(y)


class CompressedOrjsonSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x):
        return zlib.compress(
            orjson.dumps(x),
            level=3)

    @classmethod
    def deserialize(cls, y):
        with no_gc():
            return orjson.loads(zlib.decompress(y))

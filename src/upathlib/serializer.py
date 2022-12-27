import abc
import gc
import json
import pickle
import zlib
from typing import TypeVar

import orjson
import zstandard


T = TypeVar("T")


MEGABYTE = 1048576  # 1024 * 1024
ZLIB_LEVEL = 3  # official default is 6
ZSTD_LEVEL = 3  # official default is 3


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


class ZPickleSerializer(PickleSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZLIB_LEVEL, protocol=None):
        y = super().serialize(x, protocol=protocol)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = zlib.decompress(y)
        return super().deserialize(y)


class ZstdPickleSerializer(PickleSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZSTD_LEVEL, protocol=None):
        y = super().serialize(x, protocol=protocol)
        return zstandard.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = zstandard.decompress(y)
        return super().deserialize(y)


class JsonSerializer(TextSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return json.dumps(x, **kwargs)

    @classmethod
    def deserialize(cls, y, **kwargs):
        return _loads(json.loads, y, **kwargs)


# class ZJsonSerializer(ByteSerializer):
#     @classmethod
#     def serialize(cls, x, *, level=ZLIB_LEVEL, **kwargs):
#         y = json.dumps(x, **kwargs).encode()
#         return zlib.compress(y, level=level)

#     @classmethod
#     def deserialize(cls, y, **kwargs):
#         y = zlib.decompress(y).decode()
#         return _loads(json.loads, y, **kwargs)


# class ZstdJsonSerializer(ByteSerializer):
#     @classmethod
#     def serialize(cls, x, *, level=ZSTD_LEVEL, **kwargs):
#         y = json.dumps(x, **kwargs).encode()
#         return zstandard.compress(y, level=level)

#     @classmethod
#     def deserialize(cls, y, **kwargs):
#         y = zstandard.decompress(y).decode()
#         return _loads(json.loads, y, **kwargs)


class OrjsonSerializer(ByteSerializer):
    @classmethod
    def serialize(cls, x, **kwargs):
        return orjson.dumps(x, **kwargs)  # pylint: disable=no-member

    @classmethod
    def deserialize(cls, y):
        return _loads(orjson.loads, y)  # pylint: disable=no-member


class ZOrjsonSerializer(OrjsonSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZLIB_LEVEL, **kwargs):
        y = super().serialize(x, **kwargs)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = zlib.decompress(y)
        return super().deserialize(y)


class ZstdOrjsonSerializer(OrjsonSerializer):
    @classmethod
    def serialize(cls, x, *, level=ZSTD_LEVEL, **kwargs):
        y = super().serialize(x, **kwargs)
        return zstandard.compress(y, level=level)

    @classmethod
    def deserialize(cls, y):
        y = zstandard.decompress(y)
        return super().deserialize(y)

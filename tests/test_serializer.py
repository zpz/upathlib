from upathlib.serializer import (
    JsonSerializer, ZJsonSerializer, ZstdJsonSerializer,
    PickleSerializer, ZPickleSerializer, ZstdPickleSerializer,
    OrjsonSerializer, ZOrjsonSerializer, ZstdOrjsonSerializer,
)


data = [12, 23.8, {'a': [9, 'xyz'], 'b': {'first': 3, 'second': 2.3}}, None]


def test_all():
    for serde in (JsonSerializer, ZJsonSerializer, ZstdJsonSerializer,
                  PickleSerializer, ZPickleSerializer, ZstdPickleSerializer,
                  OrjsonSerializer, ZOrjsonSerializer, ZstdOrjsonSerializer,
                  ):
        y = serde.serialize(data)
        z = serde.deserialize(y)
        assert z == data

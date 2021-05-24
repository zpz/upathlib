# upathlib

This package defines some APIs for working with a cloud blob store (or object store). The intended usage is that a client package for a blob store can be built on top of these APIs by inheriting from the classes defined here.

The APIs are intentionally similar to the corresponding APIs of the standard [`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.

Local (POSIX) filesystem is treated as one particular case.

## Q & A

- What's the 'u' in the name?

  It stands for "universal". Hey, it's just a name.

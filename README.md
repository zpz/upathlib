# upathlib

This package defines some APIs for working with a cloud blob store (or object store). The intended usage is that a client package for a blob store can be built on top of these APIs by inheriting from the classes defined here.

The APIs are intentionally similar to the corresponding APIs of the standard [`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.
Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities that are non-essential in these tasks are ignored,
such as symbolic links, fine-grained permissions, and various access modes.
(This API does not require or provide for these considerations, but a subclass may still add them if desired and feasible.)

Local (POSIX) filesystem is treated as one particular case,
and is implemented by `LocalUpath`.
This class showcases the most essential methods
that a subclass needs to implement.

## Q & A

- What's the 'u' in the name?

  It stands for "universal". Hey, it's just a name.

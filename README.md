# upathlib

This package defines some APIs for working with a cloud blob store (aka "object store").

The APIs are intentionally similar to the corresponding APIs of the standard [`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.
Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a local file system that are non-essential in these tasks---such
such as symbolic links, fine-grained permissions,
 and various access modes---are ignored.

Local (POSIX) filesystem is treated as one particular case,
and is implemented by `LocalUpath`.

Clients for Azure and GCP blob stores are implemented by subclasses
`AzureBlobUpath` and `GcpBlobUpath`, respectively. End-user may want to
add very thin wrappers to handle credentials.

## Q & A

- What's the 'u' in the name?

  It stands for "universal" or "unified". Hey, it's just a name.

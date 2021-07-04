# upathlib

This package defines some APIs for working with a cloud blob store (aka "object store"). The motivation is to provide a "unified" or "universal" API for blob store clients, including local file systems.

The APIs follow the style of the standard library [`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.
Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a local file system that are non-essential in these tasks---such
such as symbolic links, fine-grained permissions,
 and various access modes---are ignored.

Local (POSIX) file system is treated as one particular case,
and is implemented by `LocalUpath`.

Clients for Azure and GCP blob stores are implemented by subclasses
`AzureBlobUpath` and `GcpBlobUpath`, respectively. Users may want to
add very thin wrappers to handle credentials. Clients for other public-cloud blob stores will be added as needed.

Each method that involves cloud service calls has both sync and async versions.

One use case is the [`biglist` package](https://github.com/zpz/biglist).
The class `Biglist` takes a `Upath` object to indicate its location of storage.
It does not care whether the storage is local, or in Azure or GCP or AWS S3 or other
cloud blob store---it simply uses the common API to operate the storage.

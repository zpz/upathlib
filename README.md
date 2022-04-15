# upathlib

This package defines some APIs for working with a cloud blob store (aka "object store"). The motivation is to provide a "unified" or "universal" API for blob store clients.

The APIs follow the style of the standard library [`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.
Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a local file system that are non-essential in these tasks---such
as symbolic links, fine-grained permissions,
 and various access modes---are ignored.

Local (POSIX) file system is treated as one particular case with the same API,
and is implemented by `LocalUpath`.

Clients for Azure and GCP blob stores are implemented by subclasses
`AzureBlobUpath` and `GcpBlobUpath`, respectively. Users may want to
add very thin wrappers in their applications to handle credentials.
Clients for other public-cloud blob stores will be added as needed.

One use case is the [`biglist` package](https://github.com/zpz/biglist).
The class `Biglist` takes a `Upath` object to indicate its location of storage.
It does not care whether the storage is local, or in Azure or GCP or AWS S3 or other
cloud blob stores---it simply uses the common API to operate the storage.

This package is published to `pypi`. To install, do one of the following:

```
$ pip3 install upathlib
$ pip3 install upathlib[azure]
$ pip3 install upathlib[gcp]
$ pip3 install upathlib[azure,gcp]
```

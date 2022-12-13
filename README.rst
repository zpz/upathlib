# upathlib

This package defines some APIs for working with a cloud blob store (aka "object store"). The motivation is to provide a "unified" or "universal" API for blob store clients.

The APIs follow the style of the standard library
[`pathlib`](https://docs.python.org/3/library/pathlib.html) where appropriate.
Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a local file system that are non-essential in these tasks---such
as symbolic links, fine-grained permissions,
 and various access modes---are ignored.

End user should look to the class `Upath` for documentation on the API.

Local (POSIX) file system is treated as one particular case with the same API,
and is implemented by `LocalUpath`, which subclasses `Upath`.

Client for Google Cloud Storage (i.e. blob store on GCP) is implemented by a `Upath` subclass
`GcsBlobUpath`. Users may want to
add very thin wrappers in their application code to handle credentials for their cloud accounts.

One use case is the package [`biglist`](https://github.com/zpz/biglist).
The class `Biglist` takes a `Upath` object to indicate its location of storage.
It does not care whether the storage is local, or in Azure or GCP or AWS S3 or other
cloud blob stores---it simply uses the common API to operate the storage.

This package is published to `pypi`. To install, do one of the following:

```
$ pip3 install upathlib
$ pip3 install upathlib[gcs]
```

(`gcs` means Google Cloud Storage, which is its official branding.)


## Status

The style of the API is largely stable. The implementations for local file system and for Google Cloud Storage are production ready.

The implementation for Azure was once in production use, but not anymore by myself, hence is unreliable.
There is a much older implementation for AWS, which was moved to "archive", because it was not updated to the current API.

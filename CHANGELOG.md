# Changelog

## Release 0.6.6 (forthcoming)

- Improvement to robustness in large directory upload to Gcp.
- Improved progress report when downloading/uploading a folder.


## Release 0.6.5

- Make GCP info to `GcpBlobUpath` optional.
- Remove home-made retrying utility `Backoff`. Use `opnieumw`.
- Bug fix related to concurrent downloading involving large files.


## Release 0.6.4

- GCP download of large blobs uses threading concurrency.
- API change: `write_bytes` and `write_text` return `None`.
- Improvements to handling of concurrency.
- Increased default concurrency level from 4 to 16.
- Simplified retry logic. For example, GCP's `download_to_file` has its own
  handling of retry; now we rely on `download_to_file` to finish the task
  and do not retry on it.
- Simplified parameters and behavior around 'overwrite'.
- Improvements to serializers:
  - Serializers allow extra arguments in their `serialize` and `deserialize` methods.
  - Added dependency `zstandard` to provide compression.
  - New serializers `ZJsonSerializer`, `ZstdJsonSerializer`, `ZstdPickleSerializer`, `ZstdOrjsonSerializer`.


## Release 0.6.3

- Use `overrides` to enforce sanity in class inheritance.
- Add tests on Azure and GCP using mocks.
- Add more retry logic to GCP.
- Refactor and simplify test/build process.


## Release 0.6.2

- GcpUpath: refresh cache upon timeout error.


## Release 0.6.1.post1

- Relax version requirements on dependencies.


## Release 0.6.1

- Minor fine-tuning.


## Release 0.6.0.post1

- Bug fix.


## Release 0.6.0

- Bug fix in GcpBlobUpath


## Release 0.5.9

- Removed async methods, since the current simplementations are simply wrappers around thread-pool executions.


## Release 0.5.8

- Refactor the `__init__` method.
- Efficiency improvements for GCP.


## Release 0.5.7

- Capture use-specified exceptions and retry in operations on multiple blobs.


## Release 0.5.6

- Bug fixes and improvements to GcpBlobUpath.


## Release 0.5.5

- Bug fix related to GCP.


## Release 0.5.4

- Implement blob locking for GCP.


## Release 0.5.3

- Improvements to 'lock' methods.


## Release 0.5.2

- Skip thread pool when `concurrency <= 1`.
- Support extra dependencies via `options.extras_require` in `setup.cfg`.


## Release 0.5.1

- Reworked `AzureBlobUpath.lock` and `AzureBlobUpath.a_lock`.


## Release 0.5.0

- `remove_file` loses argument `missing_ok`. Return 0 if file is not found.
- `GcpBlobUpath` copy, download, upload, rename.
- `BlobUpath` gets more methods for download and upload, which are thin wrappers
  around export and import methods.


## Release 0.4.9

- Another round of API fine tuning. There are some improvements to naming,
  consistency, and simplicity.
- `AzureBlobUpath` has custom blob copying, uploading, downloading.
- Improvements to tests.


## Release 0.4.5

- Remove `AzureBlobUpath` and `GcpBlobUpath` from package `__init__.py`,
  making their dependencies optional in a future release.


## Release 0.4.3

- More "native" implementation of async methods.



## Release 0.4.0

- API refinements.


## Release 0.3.7

- Bug fix.


## Release 0.3.4

- Add implementations for Azure and GCP blob stores.


## Release 0.2.2

- Add JSON and pickle convenience methods to API.


## Release 0.2.1

- Add preliminary `lock` API.


## Release 0.2.0

- API iterations.
- `LocalUpath` implementation


## Release 0.1.0

First draft of API and LocalUpath.

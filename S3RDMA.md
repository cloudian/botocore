# RDMA support for S3 (cuobj)

This fork of [botocore](https://github.com/boto/botocore) adds
RDMA-accelerated `GetObject`, `PutObject` and `UploadPart` operations for
S3, using NVIDIA's `cuobj`/GPUDirect Storage client library
(`libcuobjclient.so`). This document covers the RDMA-specific installation
steps, API additions, and runtime behavior; everything else about installing
and using botocore is unchanged from upstream - see the top-level
[README](README.rst).

## Installation

Ensure you have a modern C++ compiler and the latest CUDA toolkit installed.

To install from source:

```
pip install -e .
```

To install directly from GitHub:

```
pip install git+https://github.com/cloudian/botocore.git@1.43.111+rdma
```

## Usage

The Python package name remains `botocore` so you should not need to make any changes to your botocore based applications.

When an `s3` client is created the existing `get_object`, `put_object` and `upload_part` functions are replaced with versions that use RDMA for data transfers. 
Additionally, three new functions `get_object_direct`, `put_object_direct` and `upload_part_direct` are provided that read/write from a supplied `CuDataPtr` to allow reading/writing directly to/from bytes like objects. These memory regions can either be in host or GPU memory.

The original functions are still available as `get_object_tcp`, `put_object_tcp` and `upload_part_tcp`.


## boto3

The `boto3` package depends on `botocore` and is compatible with our RDMA enabled botocore. However, steps need to be taken to persuade package managers. First create a requirements file that pins the versions of `boto3` and `botocore`:

```
boto3==1.43.111
botocore==1.43.111
```

Install these requirements:

```
$ pip install -r requirements.txt
...
$ pip list
Package         Version
--------------- -----------
boto3           1.43.111
botocore        1.43.111
jmespath        1.0.1
pip             24.0
python-dateutil 2.9.0.post0
s3transfer      0.11.3
six             1.17.0
urllib3         2.3.0
```

Finally, install this version of botocore which will replace the vanilla `botocore` with this RDMA enabled version.

By default `boto3` will use the common runtime (CRT) transfer manager if the CRT is installed. The CRT transfer manager will not use RDMA when uploading and downloading files, whereas the Python based `s3transfer` manager uses `botocore-rdma` and so will benefit from RDMA transfers.

To force the use of the Python based transfer manager to enable RDMA you can either:
1. export `BOTO_DISABLE_CRT=true`. This will also disable additional checksum algorithms provided by the CRT
2. Provide a `boto3.s3.transfer.TransferConfig` to the upload/download methods with the `preferred_transfer_client` field set to `boto3.s3.constants.CLASSIC_TRANSFER_CLIENT`

### RDMA threshold

By default, reads and writes of less than 1MiB will continue to use TCP - RDMA will only be used for transfers 1MiB or larger. To change this default set the `S3RDMA_THRESHOLD_BYTES` environment variable.
Note: this threshold is not used for standard `get_object` requests, as the transfer size is not known at the time the request is made. However, the threshold *is* used for `get_object_direct` requests when the destination buffer size is known.

### Buffer pool

To avoid having to register a `CuDataPtr` for RDMA transfers on every request, a pool of pre-registered `CuDataBuffer`s are available. The number of buffers and size of each buffer can be controlled by the `S3RDMA_BUFFER_POOL_CAPACITY` and `S3RDMA_BUFFER_POOL_BUFSIZE_MIB` environment variables.

To get a buffer from the pool, call `buf = client.buffer_pool.get_buffer()` (where `client` is a botocore `S3Client`).
Once you've finished with the buffer, it should be returned to the pool by calling `client.buffer_pool.put_buffer(buf)` so it can be reused in future requests.

### Using `get_object_direct`/`put_object_direct`/`upload_part_direct`

The `_direct` functions take a `CuDataPtr` in place of the usual `Body` parameter (for `put_object_direct`/`upload_part_direct`) or as their first positional argument (for `get_object_direct`). A `CuDataPtr` describes a region of memory - either in host or GPU memory - that data is read from or written into. Whether RDMA is actually used for the transfer depends on the size of the `CuDataPtr` versus `S3RDMA_THRESHOLD_BYTES` - smaller transfers fall back to TCP automatically.

#### `CuDataPtr.from_buffer`

If you already have the data you want to upload in a bytes-like object (`bytes`, `bytearray`, a `numpy` array, a GPU tensor exposing the buffer protocol, etc.), wrap it with `CuDataPtr.from_buffer()` and pass it straight to `put_object_direct`/`upload_part_direct`.

A `CuDataPtr` must be registered for RDMA before it's used - buffers checked out from the buffer pool are pre-registered already (see below), but one created with `CuDataPtr.from_buffer()` isn't, so wrap its use in `client.register_buffer(ptr)`. This registers the pointer on entry and unregisters it again on exit (a no-op if the transfer is too small for RDMA and falls back to TCP):

```python
from botocore.pycuobjclient import CuDataPtr

data = b"..." # or any object that exposes the buffer protocol
ptr = CuDataPtr.from_buffer(data)

with client.register_buffer(ptr):
    client.put_object_direct(ptr, Bucket=bucket, Key=key)
```

The same pattern works for `upload_part_direct` when doing a multipart upload:

```python
upload_id = client.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]

parts = []
for part_number, chunk in enumerate(chunks, start=1):
    ptr = CuDataPtr.from_buffer(chunk)
    with client.register_buffer(ptr):
        rsp = client.upload_part_direct(
            ptr, Bucket=bucket, Key=key, UploadId=upload_id, PartNumber=part_number
        )
    parts.append({"ETag": rsp["ETag"], "PartNumber": part_number})

client.complete_multipart_upload(
    Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
)
```

#### `get_object_direct` with the buffer pool

`get_object_direct` writes into a `CuDataPtr` you supply, so the destination memory needs to be allocated (and, for RDMA transfers, registered) up front. The simplest way to get one is to check out a buffer from the client's buffer pool - `buf.data_ptr()` returns a `CuDataPtr` covering the whole buffer, and `slice(offset, length)` can be used to get a `CuDataPtr` sized to exactly how much data you expect to read:

```python
buf = client.buffer_pool.get_buffer()
try:
    dst = buf.data_ptr().slice(0, object_length)
    rsp = client.get_object_direct(dst, Bucket=bucket, Key=key)

    transferred = rsp["RdmaBytesTransferred"]
    data = memoryview(dst).tobytes()[:transferred]
finally:
    client.buffer_pool.put_buffer(buf)
```

If `object_length` is greater than the pool's buffer size (`client.buffer_pool.buffer_size()`), issue additional `get_object_direct` calls with a `Range` parameter to read the remaining bytes into further buffers - this is exactly what the higher level `get_object` does internally (see below).

### GetObject

`get_object` uses the buffer pool to read data from the server. A `RdmaBytesTransferred` field in the response (from the `x-amz-rdma-bytes-transferred` header) tells us how much data has been copied in this request; a `Content-Range` header on the first response gives the object's total size. If the transferred length is less than the total length then ranged GetObject requests are performed into additional buffers from the pool until all the data has been read. The buffers are attached to the `response['Body']` field wrapped in a stream object that can sequentially `read()` data from the buffers.


### PutObject/UploadPart

If the body length is greater than the buffer pool buffer size or cannot be determined we fall back to using tcp to transfer the object. Otherwise the body is consumed into a buffer from the pool and the equivalent `_direct` function is called to write the data to HyperStore using RDMA.


### Checksums

`put_object_direct` and `upload_part_direct` will calculate a checksum if the `ChecksumAlgorithm` field is set and the corresponding checksum hasn't been set by the caller. It will only do this if the buffer is in system memory - the caller should calculate and set the checksum for GPU allocated memory if required.

`get_object` will verify a full object checksum if `ChecksumMode` is `ENABLED` and the response contains a supported checksum algorithm (composite checksums generated by multi part uploads are not verified)


## Testing on non-RDMA capable systems

Set `S3RDMA_CLIENT_ALWAYS_USE_TCP=true` to switch the s3 client to use TCP instead of RDMA.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `S3RDMA_CLIENT_ALWAYS_USE_TCP` | unset | If set (to any non-empty value), disables RDMA and falls back to TCP for all transfers - see [Testing on non-RDMA capable systems](#testing-on-non-rdma-capable-systems) above. |
| `S3RDMA_CLIENT_NATIVE_IO` | unset | If set (to any non-empty value), disables RDMA the same way as `S3RDMA_CLIENT_ALWAYS_USE_TCP`. |
| `S3RDMA_THRESHOLD_BYTES` | `1048576` (1MiB) | Transfers smaller than this fall back to TCP; only transfers at or above this size use RDMA. Not used by plain `get_object` (the transfer size isn't known up front), but is used by `get_object_direct` - see [RDMA threshold](#rdma-threshold) above. |
| `S3RDMA_CONCURRENT_READS` | `4` | Maximum number of ranged `GetObject` requests issued concurrently when reading an object across multiple buffers. |
| `S3RDMA_BUFFER_POOL_CAPACITY` | `128` | Number of pre-registered `CuDataBuffer`s kept in the client's buffer pool - see [Buffer pool](#buffer-pool) above. |
| `S3RDMA_BUFFER_POOL_BUFSIZE_MIB` | `10` | Size, in MiB, of each buffer in the buffer pool. |
| `BOTO_DISABLE_CRT` | unset | Not defined by this fork - a `boto3`/`awscrt` flag. Set to `true` to force `boto3` onto the classic (`s3transfer`) transfer manager instead of the CRT one, since only the classic manager uses this fork's RDMA support - see [Usage](#usage) above. |

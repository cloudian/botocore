# Copyright Cloudian, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import copy
import logging
import os
from concurrent import futures
from contextlib import contextmanager
from io import IOBase

import botocore.client
import botocore.exceptions
import botocore.httpchecksum
import botocore.response
from botocore.pycuobjclient import (
    CuBufferPool,
    CuDataPtr,
    CuObjClient,
    cu_create_read_buffer,
)

log = logging.getLogger(__name__)


# Patch the json model to support RDMA
def patch_model(model):
    # add RdmaToken and RdmaBytesTransferred shape
    model['shapes']['RdmaToken'] = {"type": "string"}
    model['shapes']['RdmaReply'] = {"type": "integer"}
    model['shapes']['RdmaBytesTransferred'] = {"type": "long"}

    rdma_token_param = {
        "shape": "RdmaToken",
        "documentation": "The RDMA token for the object.",
        "location": "header",
        "locationName": "x-amz-rdma-token",
    }

    rdma_reply_param = {
        "shape": "RdmaReply",
        "documentation": "The RDMA status code in the response.",
        "location": "header",
        "locationName": "x-amz-rdma-reply",
    }

    rdma_bytes_transferred_param = {
        "shape": "RdmaBytesTransferred",
        "documentation": "The number of bytes transferred in this request",
        "location": "header",
        "locationName": "x-amz-rdma-bytes-transferred",
    }

    # Split get/put/upload part into tcp and rdma versions:
    # 1) Remove checksum calculation/verification from rdma versions
    # 2) Remove http body from rdma versions
    # 3) Add x-rdma-info header to rdma versions
    # 4) Add x-rdma-bytes-transferred header to rdma get object response

    # GetObject
    model['operations']['GetObjectTCP'] = model['operations'].pop('GetObject')
    model['operations']['GetObjectRDMA'] = copy.deepcopy(
        model['operations']['GetObjectTCP']
    )
    model['operations']['GetObjectRDMA']['input']['shape'] = (
        'GetObjectRDMARequest'
    )
    model['operations']['GetObjectRDMA']['output']['shape'] = (
        'GetObjectRDMAOutput'
    )
    del model['operations']['GetObjectRDMA']['httpChecksum']

    model['shapes']['GetObjectRDMARequest'] = copy.deepcopy(
        model['shapes']['GetObjectRequest']
    )
    model['shapes']['GetObjectRDMARequest']['members']['RdmaToken'] = (
        rdma_token_param
    )
    model['shapes']['GetObjectRDMARequest']['required'].append("RdmaToken")
    # HyperStore doesn't currently support RDMA for a specific part read.
    del model['shapes']['GetObjectRDMARequest']['members']['PartNumber']

    model['shapes']['GetObjectRDMAOutput'] = copy.deepcopy(
        model['shapes']['GetObjectOutput']
    )
    model['shapes']['GetObjectRDMAOutput']['members']['RdmaReply'] = (
        rdma_reply_param
    )
    model['shapes']['GetObjectRDMAOutput']['members'][
        'RdmaBytesTransferred'
    ] = rdma_bytes_transferred_param

    # PutObject
    model['operations']['PutObjectTCP'] = model['operations'].pop('PutObject')
    model['operations']['PutObjectRDMA'] = copy.deepcopy(
        model['operations']['PutObjectTCP']
    )
    model['operations']['PutObjectRDMA']['input']['shape'] = (
        'PutObjectRDMARequest'
    )
    model['operations']['PutObjectRDMA']['output']['shape'] = (
        'PutObjectRDMAOutput'
    )
    del model['operations']['PutObjectRDMA']['httpChecksum']

    model['shapes']['PutObjectRDMARequest'] = copy.deepcopy(
        model['shapes']['PutObjectRequest']
    )
    model['shapes']['PutObjectRDMARequest']['members']['RdmaToken'] = (
        rdma_token_param
    )
    model['shapes']['PutObjectRDMARequest']['required'].append("RdmaToken")

    model['shapes']['PutObjectRDMAOutput'] = copy.deepcopy(
        model['shapes']['PutObjectOutput']
    )
    model['shapes']['PutObjectRDMAOutput']['members']['RdmaReply'] = (
        rdma_reply_param
    )

    del model['shapes']['PutObjectRDMARequest']['members']['Body']
    del model['shapes']['PutObjectRDMARequest']['payload']

    # UploadPart
    model['operations']['UploadPartTCP'] = model['operations'].pop(
        'UploadPart'
    )
    model['operations']['UploadPartRDMA'] = copy.deepcopy(
        model['operations']['UploadPartTCP']
    )
    model['operations']['UploadPartRDMA']['input']['shape'] = (
        'UploadPartRDMARequest'
    )
    model['operations']['UploadPartRDMA']['output']['shape'] = (
        'UploadPartRDMAOutput'
    )
    del model['operations']['UploadPartRDMA']['httpChecksum']

    model['shapes']['UploadPartRDMARequest'] = copy.deepcopy(
        model['shapes']['UploadPartRequest']
    )
    model['shapes']['UploadPartRDMARequest']['members']['RdmaToken'] = (
        rdma_token_param
    )
    model['shapes']['UploadPartRDMARequest']['required'].append("RdmaToken")
    del model['shapes']['UploadPartRDMARequest']['members']['Body']
    del model['shapes']['UploadPartRDMARequest']['payload']

    model['shapes']['UploadPartRDMAOutput'] = copy.deepcopy(
        model['shapes']['UploadPartOutput']
    )
    model['shapes']['UploadPartRDMAOutput']['members']['RdmaReply'] = (
        rdma_reply_param
    )


# Patch the s3 client class to support RDMA
def inject_s3rdma(class_attributes, **kwargs):
    class_attributes['__init__'] = init_s3rdma
    class_attributes['register_buffer'] = register_buffer
    class_attributes['get_object'] = get_object
    class_attributes['put_object'] = put_object
    class_attributes['upload_part'] = upload_part
    class_attributes['get_object_direct'] = get_object_direct
    class_attributes['put_object_direct'] = put_object_direct
    class_attributes['upload_part_direct'] = upload_part_direct
    class_attributes['_rdma_body'] = _rdma_body


def make_cuobj_client():
    if os.getenv("S3RDMA_CLIENT_NATIVE_IO") or os.getenv(
        "S3RDMA_CLIENT_ALWAYS_USE_TCP"
    ):
        log.warning(
            "RDMA disabled - S3RDMA_CLIENT_NATIVE_IO or S3RDMA_CLIENT_ALWAYS_USE_TCP environment variable set"
        )
        return None

    cuobj = CuObjClient()

    if not cuobj.is_connected():
        log.warning("RDMA disabled - cuObjClient not connected")
        return None

    # Do a dummy RDMA token request to check if RDMA is available
    test_buf = cu_create_read_buffer(64)

    # Register the test buffer
    ptr = CuDataPtr.from_buffer(test_buf)

    if cuobj.register_descriptor(ptr) != 0:
        log.warning("RDMA disabled - cuObjClient register_descriptor failed")
        return None

    try:
        cuobj.get_rdma_get_token(ptr)
    except RuntimeError:
        log.warning("RDMA disabled - cuObjClient get_rdma_get_token failed")
        cuobj.release_descriptor(ptr)
        return None

    if cuobj.release_descriptor(ptr) != 0:
        log.warning("RDMA disabled - cuObjClient release_descriptor failed")
        return None

    return cuobj


def init_s3rdma(self, **kwargs):
    pool = kwargs.pop('cu_pool', None)

    # I'm not sure what we can't call super().__init__ here!
    botocore.client.BaseClient.__init__(self, **kwargs)

    self.cuobj = make_cuobj_client()
    self.max_concurrent_reads = int(
        os.environ.get("S3RDMA_CONCURRENT_READS", 4)
    )
    self.rdma_threshold = int(
        os.environ.get("S3RDMA_THRESHOLD_BYTES", 1 << 20)
    )  # 1 MiB

    if pool:
        self.buffer_pool = pool
    else:
        # Create a pool of buffers pre-registered for rdma
        nbuffers = int(os.getenv('S3RDMA_BUFFER_POOL_CAPACITY', 128))
        nbuffer_size = (
            int(os.getenv('S3RDMA_BUFFER_POOL_BUFSIZE_MIB', 10)) << 20
        )
        self.buffer_pool = CuBufferPool(self.cuobj, nbuffers, nbuffer_size)


@contextmanager
def register_buffer(self, buf):
    """A context manager that registers a buffer descriptor and unregisters it
    when the context exits."""

    # If not using cuobj or size is below threshold then no need to register a buffer
    cuobj = self.cuobj
    if cuobj is None or buf.size() < self.rdma_threshold:
        yield
        return

    try:
        cuobj.register_descriptor(buf)
        yield
    finally:
        cuobj.release_descriptor(buf)


######################################################################
# Versions of get/put/upload that read/write allocate an internal
# buffer for rdma transfer
######################################################################


def get_object(self, **kwargs):
    """Get an object from S3.
    If RDMA is enabled, the object is read directly into a CuDataPtr from the buffer pool
    and returned as a StreamingBody.
    """

    # If we're not using cuobj, we can't do rdma
    # If we're getting a specific part we can't use rdma (not currently supported by HyperStore)
    if self.cuobj is None or 'PartNumber' in kwargs:
        return self.get_object_tcp(**kwargs)

    user_supplied_range = 'Range' in kwargs

    # Allocate a buffer and read as much as we can
    buf = self.buffer_pool.get_buffer()
    try:
        rsp = self.get_object_direct(buf.data_ptr(), **kwargs)
    except Exception:
        self.buffer_pool.put_buffer(buf)
        raise

    # Check how much data has been transferred
    transferred = rsp.pop('RdmaBytesTransferred', None)
    if transferred is None:
        raise botocore.exceptions.BotoCoreError(
            report="response doesn't contain x-amz-rdma-bytes-transferred header"
        )

    if transferred == 0:
        # The only way get_object_direct() returns zero bytes transferred is
        # a zero-byte object: it retries without a range whenever the range
        # it's holding can't be satisfied, which for a non-empty object would
        # instead come back with at least one byte. There's no Content-Range
        # to parse here, since that retry drops the Range entirely.
        total = 0
    else:
        # We expect a Content-Range header in the response - get_object_direct()
        # will have inserted a Range if the original request didn't include one.
        content_range = rsp.get('ContentRange')
        if content_range is None:
            raise botocore.exceptions.BotoCoreError(
                report="response doesn't contain Content-Range header"
            )

        if user_supplied_range:
            # Size the transfer from what was actually served, not from what
            # was requested - S3 clips a range that extends past the end of
            # the object, so the response can cover fewer bytes than asked
            # for.
            served_span = content_range.removeprefix('bytes ').partition('/')[
                0
            ]
            begin_str, _, end_str = served_span.partition('-')
            total = int(end_str) - int(begin_str) + 1
        else:
            # No range was supplied, so we want to read the entire object,
            # but we don't know its size yet - get_object_direct() will have
            # bounded the range to what the buffer can hold, so the full
            # object size can be parsed from the Content-Range denominator
            # instead.
            total = int(content_range.split("/")[-1])

    rdma_body = RdmaStreamingBody(self, buf, transferred, total, kwargs, rsp)
    body = None

    # Check if we should verify checksum
    if kwargs.get('ChecksumMode') == 'ENABLED':
        for (
            algorithm,
            checksum_cls,
        ) in botocore.httpchecksum._CHECKSUM_CLS.items():
            expected_checksum = rsp.get(f'Checksum{algorithm.upper()}')
            if not expected_checksum:
                continue
            # If this is a composite checksum for an MPU it will have a -<num parts> suffix.
            # We can't verify these checksums as we don't have info about the parts
            if '-' in expected_checksum:
                break

            body = botocore.httpchecksum.StreamingChecksumBody(
                rdma_body, total, checksum_cls(), expected_checksum
            )
            break

    if body is None:
        # No checksum verification
        body = botocore.response.StreamingBody(rdma_body, total)

    rsp['ContentLength'] = total
    rsp['Body'] = body

    return rsp


def put_object(self, **kwargs):
    # Use native IO mode if rdma is disabled or empty object
    if self.cuobj is None or 'Body' not in kwargs:
        return self.put_object_tcp(**kwargs)

    buf, ptr = self._rdma_body(kwargs['Body'])
    if not buf:
        return self.put_object_tcp(**kwargs)

    del kwargs['Body']
    try:
        rsp = self.put_object_direct(ptr, **kwargs)
    finally:
        self.buffer_pool.put_buffer(buf)

    return rsp


def upload_part(self, **kwargs):
    # Use native IO mode if rdma is disabled or empty part
    if self.cuobj is None or 'Body' not in kwargs:
        return self.upload_part_tcp(**kwargs)

    buf, ptr = self._rdma_body(kwargs['Body'])
    if not buf:
        return self.upload_part_tcp(**kwargs)

    del kwargs['Body']
    try:
        rsp = self.upload_part_direct(ptr, **kwargs)
    finally:
        self.buffer_pool.put_buffer(buf)

    return rsp


def _rdma_body(self, body):
    """Comsume body into an rdma buffer if it's small enough"""
    body_len = botocore.utils.determine_content_length(body)
    if body_len is None:
        log.warning(
            "body length cannot be determined, falling back to using tcp to upload contents"
        )
        return None, None

    if body_len < self.rdma_threshold:
        return None, None

    max_rdma_size = self.buffer_pool.buffer_size()
    if body_len > max_rdma_size:
        log.warning(
            "body too large for rdma, falling back to using tcp to upload contents",
            extra={"body_len": body_len, "max_rdma_size": max_rdma_size},
        )
        return None, None

    buf = self.buffer_pool.get_buffer()
    ptr = buf.slice(0, body_len)

    if hasattr(body, 'readinto'):
        _readinto_full(body, ptr)
    else:
        if hasattr(body, 'read'):
            body = body.read()
        if isinstance(body, str):
            body = body.encode('utf-8')
        ptr.copy_from_buffer(body)

    return buf, ptr


def _readinto_full(body, ptr):
    """Read from body into ptr until ptr is completely filled.

    readinto() is permitted to return fewer bytes than requested (e.g. for
    pipes or sockets), so we must loop until the buffer is full or body is
    exhausted.
    """
    offset = 0
    size = ptr.size()
    while offset < size:
        n = body.readinto(ptr.slice(offset, size - offset))
        if not n:
            raise botocore.exceptions.BotoCoreError(
                report="body ended before expected content length was read"
            )
        offset += n


######################################################################################
# Versions of get/put/upload that read/write directly to/from a registered rdma buffer
######################################################################################


def _is_invalid_range_error(client_error):
    return client_error.response.get('Error', {}).get('Code') == 'InvalidRange'


def _use_native_io(cuobj, ptr, threshold):
    """Whether ptr should go through the plain TCP/native-IO path rather
    than RDMA.

    The native-IO path reads/writes ptr via a host-memory memcpy, so it only
    works for system (host) memory - non-system (e.g. GPU) memory must
    always go through RDMA regardless of size. If cuobj is None there's no
    way to query ptr's real memory type at all; treating it as host memory
    in that case is a documented caller responsibility (don't use
    RDMA-registered GPU buffers when RDMA is unavailable).
    """

    # If rdma is disabled, we must use TCP
    if not cuobj:
        if not CuObjClient.is_system_memory(ptr):
            raise botocore.exceptions.BotoCoreError(
                report="RDMA is not available and ptr is not system memory"
            )
        return True

    # Use TCP if the buffer is below the threshold and is system memory
    return ptr.size() < threshold and CuObjClient.is_system_memory(ptr)


def get_object_direct(self, dst: CuDataPtr, **kwargs):
    """Read an object directly into a registered buffer pointed to by buf."""

    def handle_tcp_response(rsp):
        if not CuObjClient.is_system_memory(dst):
            raise botocore.exceptions.BotoCoreError(
                report="RDMA was declined by the server for a "
                "non-system-memory destination"
            )
        total = int(rsp['ContentLength'])
        transferred = min(dst.size(), total)
        body = rsp['Body'].read(transferred)
        dst.copy_from_buffer(body)
        rsp['RdmaBytesTransferred'] = transferred
        return rsp

    if 'PartNumber' in kwargs:
        # HyperStore doesn't currently support RDMA for a specific part read.
        if not CuObjClient.is_system_memory(dst):
            raise botocore.exceptions.BotoCoreError(
                report="Cannot get a part into non-system-memory destination"
            )
        rsp = self.get_object_tcp(**kwargs)
        return handle_tcp_response(rsp)

    synthesized_range = 'Range' not in kwargs
    if synthesized_range:
        # Bound the request to what dst can actually hold, so the server can't
        # be asked (or attempt) to transfer more than we have room for.
        kwargs['Range'] = f'bytes=0-{dst.size() - 1}'

    cuobj = self.cuobj
    if _use_native_io(cuobj, dst, self.rdma_threshold):
        # Native IO mode - read the object and stream it into the buffer
        try:
            rsp = self.get_object_tcp(**kwargs)
        except botocore.exceptions.ClientError as e:
            if not (synthesized_range and _is_invalid_range_error(e)):
                raise
            # A zero-length object can't satisfy any byte range - retry
            # without the range we added ourselves.
            del kwargs['Range']
            rsp = self.get_object_tcp(**kwargs)
        return handle_tcp_response(rsp)

    # Keep the RdmaToken object alive (in rdma_token) until get_object_rdma
    # below returns or raises: the server performs the actual RDMA transfer
    # against it while handling that request, so releasing it any earlier
    # (e.g. by only keeping the str() we hand to kwargs) could invalidate it
    # out from under an in-progress transfer. The `finally: del` (rather than
    # just letting rdma_token go out of scope) guarantees that release the
    # moment we're done with it, even if an exception propagates out of this
    # function - an escaping exception's traceback would otherwise keep this
    # frame, and everything in it, alive for as long as someone holds onto
    # the exception.
    rdma_token = cuobj.get_rdma_get_token(dst)
    kwargs['RdmaToken'] = str(rdma_token)
    try:
        rsp = self.get_object_rdma(**kwargs)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NotEnabled':
            log.warning(
                "get_object_direct: RDMA not enabled, falling back to TCP"
            )
            del kwargs['RdmaToken']
            self.cuobj = None
            return self.get_object_direct(dst, **kwargs)
        if synthesized_range and _is_invalid_range_error(e):
            # A zero-length object can't satisfy any byte range - retry
            # without the range we added ourselves.
            del kwargs['RdmaToken']
            del kwargs['Range']
            rsp = self.get_object_tcp(**kwargs)
            return handle_tcp_response(rsp)
        raise
    finally:
        del rdma_token

    if rsp.get('RdmaReply') not in (200, 204, 206):
        # If the server hasn't accepted RDMA, the contents will be in the TCP stream instead.
        log.warning("get_object_direct: RDMA not enabled, falling back to TCP")
        self.cuobj = None
        return handle_tcp_response(rsp)

    # RdmaBytesTransferred is server-reported and otherwise unvalidated - bound
    # it to dst's actual size so a corrupted/malicious value can't drive an
    # out-of-bounds read of dst downstream (e.g. in RdmaStreamingBody).
    transferred = rsp.get('RdmaBytesTransferred')
    if transferred is None or not (0 <= transferred <= dst.size()):
        raise botocore.exceptions.BotoCoreError(
            report=f"RDMA get returned an invalid RdmaBytesTransferred: {transferred!r}"
        )
    return rsp


def put_object_direct(self, src: CuDataPtr, **kwargs):
    # Use native IO mode if rdma is disabled or buffer is too small for rdma
    cuobj = self.cuobj
    if _use_native_io(cuobj, src, self.rdma_threshold):
        with memoryview(src) as body:
            return self.put_object_tcp(**kwargs, Body=body.tobytes())

    def tcp_fallback():
        log.warning("put_object_direct: RDMA not enabled, falling back to TCP")
        self.cuobj = None
        del kwargs['RdmaToken']
        return self.put_object_direct(src, **kwargs)

    _calculate_checksum(src, kwargs)
    # See the comment in get_object_direct: rdma_token must stay alive until
    # put_object_rdma below returns or raises, and the `finally: del` there
    # guarantees prompt release on every exit path, including one that
    # raises out of this function.
    rdma_token = cuobj.get_rdma_put_token(src)
    kwargs['RdmaToken'] = str(rdma_token)
    try:
        rsp = self.put_object_rdma(**kwargs)
        if rsp.get('RdmaReply') not in (200, 204):
            return tcp_fallback()
        return rsp
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NotEnabled':
            return tcp_fallback()
        raise
    finally:
        del rdma_token


def upload_part_direct(self, src: CuDataPtr, **kwargs):
    # Use native IO mode if rdma is disabled or buffer is too small for rdma
    cuobj = self.cuobj
    if _use_native_io(cuobj, src, self.rdma_threshold):
        with memoryview(src) as body:
            return self.upload_part_tcp(**kwargs, Body=body.tobytes())

    def tcp_fallback():
        log.warning(
            "upload_part_direct: RDMA not enabled, falling back to TCP"
        )
        self.cuobj = None
        del kwargs['RdmaToken']
        return self.upload_part_direct(src, **kwargs)

    _calculate_checksum(src, kwargs)
    # See the comment in get_object_direct: rdma_token must stay alive until
    # upload_part_rdma below returns or raises, and the `finally: del` there
    # guarantees prompt release on every exit path, including one that
    # raises out of this function.
    rdma_token = cuobj.get_rdma_put_token(src)
    kwargs['RdmaToken'] = str(rdma_token)
    try:
        rsp = self.upload_part_rdma(**kwargs)
        if rsp.get('RdmaReply') not in (200, 204):
            return tcp_fallback()
        return rsp
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NotEnabled':
            return tcp_fallback()
        raise
    finally:
        del rdma_token


def _calculate_checksum(src: CuDataPtr, req):
    # Compute checksum if algorithm is specified and checksum is not provided
    algorithm_name = req.get('ChecksumAlgorithm', None)
    csum_field = 'Checksum' + algorithm_name if algorithm_name else None
    if algorithm_name and csum_field not in req:
        csum_class = botocore.httpchecksum._CHECKSUM_CLS.get(
            algorithm_name.lower(), None
        )
        if not csum_class:
            raise botocore.exceptions.FlexibleChecksumError(
                error_msg=f"Unsupported checksum algorithm: {algorithm_name}"
            )
        if not CuObjClient.is_system_memory(src):
            raise botocore.exceptions.FlexibleChecksumError(
                error_msg="Object data must be in system memory to calculate checksum"
            )
        with memoryview(src) as body:
            checksummer = csum_class()
            checksummer.update(body)
            req[csum_field] = checksummer.b64digest()


######################################################################################
# A get object response body from a set of RDMA buffers
######################################################################################


class RdmaStreamingBody(IOBase):
    def __init__(self, client, buf, transferred, total, req, rsp):
        self.client = client
        self.cursor = 0
        self.total = total

        # The current buffer being consumed
        self.buf = buf
        self.buf_cursor = 0
        self.buf_end = transferred

        # A list of futures providing additional chunks using range reads
        self.chunks = []
        self.thread_pool = None

        # Check if additional reads are required
        if transferred != total:
            if transferred == 0:
                # Without at least one byte transferred we have no way to
                # make progress with further ranged reads.
                raise botocore.exceptions.BotoCoreError(
                    report="rdma get_object transferred 0 of "
                    f"{total} expected bytes"
                )

            # Ensure additional reads use the same object
            if 'VersionId' in rsp:
                req['VersionId'] = rsp['VersionId']
            else:
                req['IfMatch'] = rsp['ETag']

            begin = 0
            if 'ContentRange' in rsp:
                # We've read a range of the object, adjust our start offset
                begin = int(
                    rsp['ContentRange']
                    .removeprefix('bytes ')
                    .partition('-')[0]
                )
            end = begin + total

            self.thread_pool = futures.ThreadPoolExecutor(
                max_workers=client.max_concurrent_reads
            )
            offset = begin + transferred  # skip over bytes we've already read
            while offset < end:
                next_offset = min(offset + transferred, end)
                chunk = self.thread_pool.submit(
                    lambda offset=offset, next_offset=next_offset: (
                        self._read_chunk(req, offset, next_offset)
                    )
                )
                self.chunks.append(chunk)
                offset = next_offset
            # We'll pop chunks from the end, so reverse the order
            self.chunks.reverse()

    def _read_chunk(self, req, begin, end):
        # Take a copy of request so we can insert a range
        req = copy.copy(req)
        req['Range'] = f"bytes={begin}-{end - 1}"
        buf = self.client.buffer_pool.get_buffer()
        try:
            rsp = self.client.get_object_direct(buf.data_ptr(), **req)
        except Exception:
            self.client.buffer_pool.put_buffer(buf)
            raise

        # We expect to be able to transfer the entire requested range
        transferred = rsp.get('RdmaBytesTransferred')
        if transferred != end - begin:
            raise botocore.exceptions.BotoCoreError(
                report="failed to transfer entire range"
            )

        return buf

    def read(self, amt=None):
        if amt is None or amt < 0:
            amt = self.total - self.cursor
        else:
            amt = min(amt, self.total - self.cursor)

        if amt == 0:
            return b''

        dst = cu_create_read_buffer(amt)
        dst_off = 0
        while amt:
            # Copy from current buffer
            to_copy = min(self.buf_end - self.buf_cursor, amt)
            self.buf.slice(self.buf_cursor, to_copy).copy_to_buffer(
                dst, dst_off
            )
            dst_off += to_copy
            self.buf_cursor += to_copy
            amt -= to_copy

            # Move to next buffer if need be
            if self.buf_cursor == self.buf_end:
                self.client.buffer_pool.put_buffer(self.buf)
                self.buf_cursor = 0
                self.buf = self.chunks.pop().result() if self.chunks else None

        self.cursor += len(dst)
        return dst

    def tell(self):
        return self.cursor

    def close(self):
        if self.buf:
            self.client.buffer_pool.put_buffer(self.buf)
            self.buf = None

        while self.chunks:
            try:
                buf = self.chunks.pop().result()
                self.client.buffer_pool.put_buffer(buf)
            except Exception:
                pass

        if self.thread_pool is not None:
            self.thread_pool.shutdown()
            self.thread_pool = None

    def __del__(self):
        # Ensure buffers are returned to the pool if user doesn't close() the stream
        self.close()

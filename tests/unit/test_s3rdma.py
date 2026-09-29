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
import types

import botocore.exceptions
import botocore.httpchecksum
from botocore import s3rdma
from tests import mock, unittest


class FakeCuDataPtr:
    """A minimal stand-in for botocore.pycuobjclient.CuDataPtr."""

    def __init__(self, size, data=None):
        self._size = size
        self._data = bytearray(data if data is not None else size)
        self.copied = None

    def size(self):
        return self._size

    def copy_from_buffer(self, buf):
        self.copied = bytes(buf)

    def __buffer__(self, flags):
        # Lets memoryview(ptr) work, for put_object_direct()'s native-IO path.
        return memoryview(self._data)


class FakeBuffer:
    """A minimal stand-in for a buffer handed out by CuBufferPool."""

    def __init__(self, size):
        self._ptr = FakeCuDataPtr(size)

    def data_ptr(self):
        return self._ptr


class FakeBody:
    """A minimal stand-in for a botocore StreamingBody."""

    def __init__(self, data):
        self._data = data

    def read(self, amt=None):
        if amt is None:
            amt = len(self._data)
        chunk, self._data = self._data[:amt], self._data[amt:]
        return chunk


class FakeS3RdmaClient:
    """A minimal stand-in for the RDMA-patched S3 client.

    Binds the module functions under test the same way inject_s3rdma() binds
    them onto the real S3 client class, while stubbing out the underlying
    TCP/RDMA wire operations so the RDMA control-flow can be exercised
    without any real hardware, daemon, or network access.
    """

    def __init__(
        self,
        cuobj=None,
        rdma_threshold=1024 * 1024,
        buffer_pool=None,
        max_concurrent_reads=4,
    ):
        self.cuobj = cuobj
        self.rdma_threshold = rdma_threshold
        self.buffer_pool = buffer_pool
        self.max_concurrent_reads = max_concurrent_reads

        self.get_object_tcp = mock.Mock(name='get_object_tcp')
        self.get_object_rdma = mock.Mock(name='get_object_rdma')
        self.put_object_tcp = mock.Mock(name='put_object_tcp')
        self.put_object_rdma = mock.Mock(name='put_object_rdma')
        self.upload_part_tcp = mock.Mock(name='upload_part_tcp')
        self.upload_part_rdma = mock.Mock(name='upload_part_rdma')

        self.get_object = types.MethodType(s3rdma.get_object, self)
        self.get_object_direct = types.MethodType(
            s3rdma.get_object_direct, self
        )
        self.put_object_direct = types.MethodType(
            s3rdma.put_object_direct, self
        )
        self.upload_part_direct = types.MethodType(
            s3rdma.upload_part_direct, self
        )


def client_error(code):
    return botocore.exceptions.ClientError(
        {'Error': {'Code': code, 'Message': 'stub'}}, 'GetObjectRDMA'
    )


class TestGetObjectDirect(unittest.TestCase):
    def setUp(self):
        # A threshold of 1 means any non-empty dst takes the RDMA path by
        # default; test_uses_tcp_when_below_rdma_threshold raises it back up
        # to specifically exercise the below-threshold TCP path.
        self.client = FakeS3RdmaClient(
            cuobj=mock.Mock(name='cuobj'), rdma_threshold=1
        )
        # is_system_memory() is a static method on the real CuObjClient
        # class (see pycuobjclient.cpp), so production code calls it there
        # directly rather than through self.client.cuobj - patch it there too.
        patcher = mock.patch.object(
            s3rdma.CuObjClient, 'is_system_memory', return_value=True
        )
        self.is_system_memory = patcher.start()
        self.addCleanup(patcher.stop)

    def test_uses_tcp_when_cuobj_is_none(self):
        self.client.cuobj = None
        dst = FakeCuDataPtr(10)
        self.client.get_object_tcp.return_value = {
            'ContentLength': 10,
            'Body': FakeBody(b'0123456789'),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_called_once_with(
            Bucket='b', Key='k', Range='bytes=0-9'
        )
        self.client.get_object_rdma.assert_not_called()
        self.assertEqual(dst.copied, b'0123456789')
        self.assertEqual(rsp['RdmaBytesTransferred'], 10)

    def test_uses_tcp_when_below_rdma_threshold(self):
        self.client.rdma_threshold = 1024
        dst = FakeCuDataPtr(10)  # below rdma_threshold=1024
        self.client.get_object_tcp.return_value = {
            'ContentLength': 10,
            'Body': FakeBody(b'0123456789'),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_rdma.assert_not_called()
        self.assertEqual(rsp['RdmaBytesTransferred'], 10)

    def test_non_system_memory_below_threshold_still_uses_rdma(self):
        # The native-IO fallback can't safely memcpy into GPU memory, so a
        # below-threshold dst must still go through RDMA if it isn't system
        # memory.
        self.client.rdma_threshold = 1024
        self.is_system_memory.return_value = False
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 200,
            'RdmaBytesTransferred': 10,
        }

        self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_not_called()
        self.client.get_object_rdma.assert_called_once()

    def test_declined_reply_for_non_system_memory_raises(self):
        # A decline means the data is in the TCP stream, but that can't be
        # copied into non-system (e.g. GPU) memory via memcpy.
        self.is_system_memory.return_value = False
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {'RdmaReply': 0}

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_not_called()

    def test_not_enabled_for_non_system_memory_raises(self):
        self.is_system_memory.return_value = False
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.side_effect = client_error('NotEnabled')

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_not_called()

    def test_bounds_range_to_dst_size_when_not_supplied(self):
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 200,
            'RdmaBytesTransferred': 2048,
        }

        self.client.get_object_direct(dst, Bucket='b', Key='k')

        _, kwargs = self.client.get_object_rdma.call_args
        self.assertEqual(kwargs['Range'], 'bytes=0-2047')

    def test_preserves_caller_supplied_range(self):
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 200,
            'RdmaBytesTransferred': 100,
        }

        self.client.get_object_direct(
            dst, Bucket='b', Key='k', Range='bytes=10-109'
        )

        _, kwargs = self.client.get_object_rdma.call_args
        self.assertEqual(kwargs['Range'], 'bytes=10-109')

    def test_accepted_rdma_reply_returns_response_unchanged(self):
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        rsp_in = {'RdmaReply': 206, 'RdmaBytesTransferred': 1024}
        self.client.get_object_rdma.return_value = rsp_in

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)

    def test_declined_reply_falls_back_to_tcp_body_already_in_response(self):
        # A decline doesn't trigger a second request - the TCP body is
        # already present in the RDMA response.
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 0,
            'ContentLength': 10,
            'Body': FakeBody(b'0123456789'),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_not_called()
        self.assertEqual(dst.copied, b'0123456789')
        self.assertEqual(rsp['RdmaBytesTransferred'], 10)
        self.assertIsNone(self.client.cuobj)

    def test_missing_rdma_reply_falls_back_to_tcp(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'ContentLength': 10,
            'Body': FakeBody(b'0123456789'),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.assertEqual(rsp['RdmaBytesTransferred'], 10)

    def test_rejects_transferred_larger_than_dst(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 200,
            'RdmaBytesTransferred': 999999,
        }

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

    def test_rejects_missing_bytes_transferred(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {'RdmaReply': 200}

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

    def test_not_enabled_client_error_falls_back_and_retries_over_tcp(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.side_effect = client_error('NotEnabled')
        self.client.get_object_tcp.return_value = {
            'ContentLength': 10,
            'Body': FakeBody(b'0123456789'),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.assertIsNone(self.client.cuobj)
        self.assertEqual(rsp['RdmaBytesTransferred'], 10)
        self.client.get_object_tcp.assert_called_once()
        self.assertNotIn(
            'RdmaToken', self.client.get_object_tcp.call_args.kwargs
        )

    def test_other_client_errors_propagate(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.side_effect = client_error('AccessDenied')

        with self.assertRaises(botocore.exceptions.ClientError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

    def test_part_number_uses_tcp_even_with_cuobj(self):
        # HyperStore doesn't support RDMA for a specific part read, even
        # when cuobj is available and dst is above the RDMA threshold.
        dst = FakeCuDataPtr(2048)
        self.client.get_object_tcp.return_value = {
            'ContentLength': 2048,
            'Body': FakeBody(b'x' * 2048),
        }

        rsp = self.client.get_object_direct(
            dst, Bucket='b', Key='k', PartNumber=2
        )

        self.client.get_object_rdma.assert_not_called()
        self.assertEqual(rsp['RdmaBytesTransferred'], 2048)
        _, kwargs = self.client.get_object_tcp.call_args
        self.assertNotIn('Range', kwargs)

    def test_rejects_negative_transferred(self):
        dst = FakeCuDataPtr(10)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.return_value = {
            'RdmaReply': 200,
            'RdmaBytesTransferred': -1,
        }

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object_direct(dst, Bucket='b', Key='k')

    def test_empty_object_retries_without_range_over_tcp(self):
        # Below the RDMA threshold, so this exercises the native-IO branch.
        self.client.rdma_threshold = 1024
        dst = FakeCuDataPtr(10)
        self.client.get_object_tcp.side_effect = [
            client_error('InvalidRange'),
            {'ContentLength': 0, 'Body': FakeBody(b'')},
        ]

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.assertEqual(self.client.get_object_tcp.call_count, 2)
        first_kwargs = self.client.get_object_tcp.call_args_list[0].kwargs
        second_kwargs = self.client.get_object_tcp.call_args_list[1].kwargs
        self.assertEqual(first_kwargs['Range'], 'bytes=0-9')
        self.assertNotIn('Range', second_kwargs)
        self.assertEqual(rsp['RdmaBytesTransferred'], 0)

    def test_empty_object_retries_without_range_after_rdma_invalid_range(
        self,
    ):
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.side_effect = client_error('InvalidRange')
        self.client.get_object_tcp.return_value = {
            'ContentLength': 0,
            'Body': FakeBody(b''),
        }

        rsp = self.client.get_object_direct(dst, Bucket='b', Key='k')

        self.client.get_object_tcp.assert_called_once()
        _, kwargs = self.client.get_object_tcp.call_args
        self.assertNotIn('Range', kwargs)
        self.assertNotIn('RdmaToken', kwargs)
        self.assertEqual(rsp['RdmaBytesTransferred'], 0)
        # InvalidRange isn't a decline - RDMA stays enabled for next time.
        self.assertIsNotNone(self.client.cuobj)

    def test_caller_supplied_range_invalid_range_error_propagates(self):
        # Only a range get_object_direct() synthesized itself triggers the
        # empty-object retry - an explicit caller range that turns out to be
        # invalid should surface as a normal error.
        dst = FakeCuDataPtr(2048)
        self.client.cuobj.get_rdma_get_token.return_value = 'token'
        self.client.get_object_rdma.side_effect = client_error('InvalidRange')

        with self.assertRaises(botocore.exceptions.ClientError):
            self.client.get_object_direct(
                dst, Bucket='b', Key='k', Range='bytes=0-99'
            )


class TestGetObject(unittest.TestCase):
    def setUp(self):
        self.buf = FakeBuffer(2 * 1024 * 1024)
        self.buffer_pool = mock.Mock(name='buffer_pool')
        self.buffer_pool.get_buffer.return_value = self.buf
        self.client = FakeS3RdmaClient(
            cuobj=mock.Mock(name='cuobj'), buffer_pool=self.buffer_pool
        )
        self.client.get_object_direct = mock.Mock(name='get_object_direct')

    def test_uses_tcp_when_cuobj_is_none(self):
        self.client.cuobj = None
        self.client.get_object_tcp.return_value = {'Body': 'stub'}

        rsp = self.client.get_object(Bucket='b', Key='k')

        self.assertEqual(rsp, {'Body': 'stub'})
        self.client.get_object_direct.assert_not_called()

    def test_uses_tcp_for_part_number(self):
        self.client.get_object_tcp.return_value = {'Body': 'stub'}

        rsp = self.client.get_object(Bucket='b', Key='k', PartNumber=2)

        self.assertEqual(rsp, {'Body': 'stub'})
        self.client.get_object_direct.assert_not_called()

    def test_no_range_sizes_transfer_from_content_range_header(self):
        # The whole object (1024 bytes) is transferred in a single read, so
        # the total comes from the response's Content-Range denominator.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 1024,
            'ContentRange': 'bytes 0-1023/1024',
        }

        rsp = self.client.get_object(Bucket='b', Key='k')

        self.assertEqual(rsp['ContentLength'], 1024)
        self.assertNotIn('RdmaBytesTransferred', rsp)
        self.buffer_pool.get_buffer.assert_called_once()

    def test_bounded_range_sizes_transfer_from_served_span(self):
        # The response's Content-Range denominator (5000, the whole object)
        # must be ignored - only the served 100-byte span was returned.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 100,
            'ContentRange': 'bytes 100-199/5000',
        }

        rsp = self.client.get_object(
            Bucket='b', Key='k', Range='bytes=100-199'
        )

        self.assertEqual(rsp['ContentLength'], 100)

    def test_range_clipped_by_server_sizes_from_served_span_not_request(
        self,
    ):
        # The object is only 150 bytes; S3 clips a request for bytes=100-999
        # down to what's actually available (bytes 100-149/150). total must
        # reflect what was served (50 bytes), not the 900 bytes requested -
        # otherwise RdmaStreamingBody would try to read past the object's end.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 50,
            'ContentRange': 'bytes 100-149/150',
        }

        rsp = self.client.get_object(
            Bucket='b', Key='k', Range='bytes=100-999'
        )

        self.assertEqual(rsp['ContentLength'], 50)

    def test_open_ended_range_sizes_from_served_span(self):
        # Open-ended and suffix ranges aren't pre-validated or rejected - S3
        # resolves them to an absolute served span in the response
        # regardless of how the request expressed the range.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 50,
            'ContentRange': 'bytes 100-149/150',
        }

        rsp = self.client.get_object(Bucket='b', Key='k', Range='bytes=100-')

        self.client.get_object_direct.assert_called_once()
        self.assertEqual(rsp['ContentLength'], 50)

    def test_multi_range_without_content_range_raises(self):
        # A multi-range request isn't rejected up front either - if the
        # server doesn't return the Content-Range header we expect for it,
        # that surfaces as a clear error rather than a silently wrong size.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 150,
        }

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object(
                Bucket='b', Key='k', Range='bytes=0-99,200-299'
            )

    def test_missing_rdma_bytes_transferred_raises(self):
        self.client.get_object_direct.return_value = {
            'ContentRange': 'bytes 0-1023/1024',
        }

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object(Bucket='b', Key='k')

    def test_missing_content_range_falls_back_to_content_length(self):
        # get_object_direct() retries without a range for an empty object,
        # so the response has no Content-Range at all - total then comes
        # from Content-Length instead.
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 0,
            'ContentLength': 0,
        }

        rsp = self.client.get_object(Bucket='b', Key='k')

        self.assertEqual(rsp['ContentLength'], 0)

    def test_missing_content_range_and_content_length_raises(self):
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 1024,
        }

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.get_object(Bucket='b', Key='k')

    def test_buffer_is_returned_to_pool_on_error(self):
        self.client.get_object_direct.side_effect = RuntimeError('boom')

        with self.assertRaises(RuntimeError):
            self.client.get_object(Bucket='b', Key='k')

        self.buffer_pool.put_buffer.assert_called_once_with(self.buf)

    def test_checksum_verification_wraps_body_when_enabled(self):
        self.client.get_object_direct.return_value = {
            'RdmaBytesTransferred': 5,
            'ContentRange': 'bytes 0-4/5',
            'ChecksumSHA256': 'c3R1YmRpZ2VzdA==',
        }

        rsp = self.client.get_object(
            Bucket='b', Key='k', ChecksumMode='ENABLED'
        )

        self.assertIsInstance(
            rsp['Body'], botocore.httpchecksum.StreamingChecksumBody
        )


class TestPutObjectDirect(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3RdmaClient(
            cuobj=mock.Mock(name='cuobj'), rdma_threshold=1
        )
        self.client.cuobj.get_rdma_put_token.return_value = 'token'
        # is_system_memory() is a static method on the real CuObjClient
        # class (see pycuobjclient.cpp), so production code calls it there
        # directly rather than through self.client.cuobj - patch it there too.
        patcher = mock.patch.object(
            s3rdma.CuObjClient, 'is_system_memory', return_value=True
        )
        self.is_system_memory = patcher.start()
        self.addCleanup(patcher.stop)

    def test_accepts_200_reply(self):
        src = FakeCuDataPtr(10)
        rsp_in = {'RdmaReply': 200}
        self.client.put_object_rdma.return_value = rsp_in

        rsp = self.client.put_object_direct(src, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)
        self.client.put_object_tcp.assert_not_called()

    def test_accepts_204_reply(self):
        # 204 No Content is a valid success reply for a PUT - it must not be
        # treated as a decline and re-uploaded over TCP.
        src = FakeCuDataPtr(10)
        rsp_in = {'RdmaReply': 204}
        self.client.put_object_rdma.return_value = rsp_in

        rsp = self.client.put_object_direct(src, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)
        self.client.put_object_tcp.assert_not_called()

    def test_missing_reply_header_falls_back_to_tcp(self):
        # A response missing the optional RdmaReply header must fall back
        # to TCP, not raise a KeyError.
        src = FakeCuDataPtr(10)
        self.client.put_object_rdma.return_value = {}
        self.client.put_object_tcp.return_value = {'ETag': 'stub'}

        rsp = self.client.put_object_direct(src, Bucket='b', Key='k')

        self.assertEqual(rsp, {'ETag': 'stub'})
        self.assertIsNone(self.client.cuobj)

    def test_true_decline_falls_back_to_tcp(self):
        src = FakeCuDataPtr(10)
        self.client.put_object_rdma.return_value = {'RdmaReply': 400}
        self.client.put_object_tcp.return_value = {'ETag': 'stub'}

        rsp = self.client.put_object_direct(src, Bucket='b', Key='k')

        self.assertEqual(rsp, {'ETag': 'stub'})
        self.assertIsNone(self.client.cuobj)

    def test_non_system_memory_below_threshold_still_uses_rdma(self):
        self.client.rdma_threshold = 1024
        self.is_system_memory.return_value = False
        src = FakeCuDataPtr(10)
        rsp_in = {'RdmaReply': 200}
        self.client.put_object_rdma.return_value = rsp_in

        rsp = self.client.put_object_direct(src, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)
        self.client.put_object_tcp.assert_not_called()

    def test_decline_for_non_system_memory_raises(self):
        # memoryview(src) below can't safely read non-system (e.g. GPU)
        # memory, so a decline against it can't be retried over TCP.
        self.is_system_memory.return_value = False
        src = FakeCuDataPtr(10)
        self.client.put_object_rdma.return_value = {'RdmaReply': 400}

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.put_object_direct(src, Bucket='b', Key='k')

        self.client.put_object_tcp.assert_not_called()


class TestUploadPartDirect(unittest.TestCase):
    def setUp(self):
        self.client = FakeS3RdmaClient(
            cuobj=mock.Mock(name='cuobj'), rdma_threshold=1
        )
        self.client.cuobj.get_rdma_put_token.return_value = 'token'
        # is_system_memory() is a static method on the real CuObjClient
        # class (see pycuobjclient.cpp), so production code calls it there
        # directly rather than through self.client.cuobj - patch it there too.
        patcher = mock.patch.object(
            s3rdma.CuObjClient, 'is_system_memory', return_value=True
        )
        self.is_system_memory = patcher.start()
        self.addCleanup(patcher.stop)

    def test_accepts_204_reply(self):
        src = FakeCuDataPtr(10)
        rsp_in = {'RdmaReply': 204}
        self.client.upload_part_rdma.return_value = rsp_in

        rsp = self.client.upload_part_direct(src, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)
        self.client.upload_part_tcp.assert_not_called()

    def test_missing_reply_header_falls_back_to_tcp(self):
        src = FakeCuDataPtr(10)
        self.client.upload_part_rdma.return_value = {}
        self.client.upload_part_tcp.return_value = {'ETag': 'stub'}

        rsp = self.client.upload_part_direct(src, Bucket='b', Key='k')

        self.assertEqual(rsp, {'ETag': 'stub'})
        self.assertIsNone(self.client.cuobj)

    def test_non_system_memory_below_threshold_still_uses_rdma(self):
        self.client.rdma_threshold = 1024
        self.is_system_memory.return_value = False
        src = FakeCuDataPtr(10)
        rsp_in = {'RdmaReply': 200}
        self.client.upload_part_rdma.return_value = rsp_in

        rsp = self.client.upload_part_direct(src, Bucket='b', Key='k')

        self.assertIs(rsp, rsp_in)
        self.client.upload_part_tcp.assert_not_called()

    def test_decline_for_non_system_memory_raises(self):
        self.is_system_memory.return_value = False
        src = FakeCuDataPtr(10)
        self.client.upload_part_rdma.return_value = {'RdmaReply': 400}

        with self.assertRaises(botocore.exceptions.BotoCoreError):
            self.client.upload_part_direct(src, Bucket='b', Key='k')

        self.client.upload_part_tcp.assert_not_called()


if __name__ == '__main__':
    unittest.main()

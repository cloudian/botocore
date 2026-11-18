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
import logging
import os
import re
import unittest
import uuid

import botocore.session
from botocore.pycuobjclient import CuDataPtr

# Header names whose values must never reach logs or captured output: live
# credentials (including temporary session tokens) and the RDMA token, which
# grants direct RDMA access to a registered memory region.
_SENSITIVE_HEADER_PATTERN = re.compile(
    r"(?i)('(?:authorization|x-amz-security-token|x-amz-rdma-token)':"
    r"\s*)'[^']*'"
)


class _RedactSensitiveHeaders(logging.Filter):
    """Redacts credential and RDMA-token header values from log records."""

    def filter(self, record):
        if record.args:
            record.msg = record.getMessage()
            record.args = ()
        if isinstance(record.msg, str):
            record.msg = _SENSITIVE_HEADER_PATTERN.sub(
                r"\1'<redacted>'", record.msg
            )
        return True


def _maybe_enable_wire_logging(session):
    """Enable full botocore wire logging on session, if explicitly opted into.

    This is separate from S3RDMA_RUN_FUNCTIONAL_TESTS because the logged
    AWSPreparedRequest includes every header, including live credentials and
    the RDMA token - sensitive header values are redacted regardless, but
    logging is still opt-in so it's never captured (e.g. to a file) by
    surprise.
    """
    if not os.environ.get("S3RDMA_DEBUG_WIRE_LOGGING"):
        return
    logger = logging.getLogger("botocore")
    if not any(isinstance(f, _RedactSensitiveHeaders) for f in logger.filters):
        logger.addFilter(_RedactSensitiveHeaders())
    session.set_debug_logger()


@unittest.skipUnless(
    os.environ.get("S3RDMA_RUN_FUNCTIONAL_TESTS"),
    "requires a live RDMA-capable S3-compatible endpoint; set "
    "S3RDMA_RUN_FUNCTIONAL_TESTS=1 (and AWS_ENDPOINT_URL/AWS_REGION as "
    "needed) to run these tests",
)
class BaseS3OperationTest(unittest.TestCase):
    def setUp(self):
        self.region = os.environ.get("AWS_REGION", "region-2")
        self.endpoint = os.environ.get(
            "AWS_ENDPOINT_URL", "http://localhost:8000"
        )
        session = botocore.session.get_session()
        _maybe_enable_wire_logging(session)
        self.client = session.create_client(
            "s3", self.region, endpoint_url=self.endpoint
        )
        self.bucket_name = f"test-rdma-{uuid.uuid4().hex[:20]}"

        self.client.create_bucket(Bucket=self.bucket_name)

    def tearDown(self):
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name):
            keys = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if keys:
                self.client.delete_objects(
                    Bucket=self.bucket_name, Delete={"Objects": keys}
                )

        self.client.delete_bucket(Bucket=self.bucket_name)
        super().tearDown()


class TestS3RdmaGetObject(BaseS3OperationTest):
    def setUp(self):
        super().setUp()
        self.key = "get-object-test.bin"
        self.data = os.urandom(
            2 * 1024 * 1024
        )  # 2 MiB - clears the default RDMA threshold

        self.client.put_object_tcp(
            Body=self.data, Bucket=self.bucket_name, Key=self.key
        )

    def test_s3_rdma_get_object_direct(self):
        buf = self.client.buffer_pool.get_buffer()
        try:
            dst = buf.data_ptr().slice(0, len(self.data))
            response = self.client.get_object_direct(
                dst, Bucket=self.bucket_name, Key=self.key
            )

            transferred = response["RdmaBytesTransferred"]
            self.assertEqual(len(self.data), transferred)
            self.assertEqual(self.data, memoryview(dst).tobytes())
        finally:
            self.client.buffer_pool.put_buffer(buf)

    def test_s3_rdma_get_object(self):
        response = self.client.get_object(
            Bucket=self.bucket_name, Key=self.key
        )
        self.assertEqual(self.data, response["Body"].read())


class TestS3RdmaPutObject(BaseS3OperationTest):
    def setUp(self):
        super().setUp()
        self.key = "put-object-test.bin"
        self.data = os.urandom(
            2 * 1024 * 1024
        )  # 2 MiB - clears the default RDMA threshold

    def test_s3_rdma_put_object(self):
        ptr = CuDataPtr.from_buffer(self.data)
        with self.client.register_buffer(ptr):
            self.client.put_object_direct(
                ptr, Bucket=self.bucket_name, Key=self.key
            )

        response = self.client.get_object_tcp(
            Bucket=self.bucket_name, Key=self.key
        )
        self.assertEqual(self.data, response["Body"].read())


class TestS3RdmaUploadPart(BaseS3OperationTest):
    def setUp(self):
        super().setUp()
        self.key = "multipart-test.bin"

        # First part must meet S3's 5 MiB minimum part size (and also clears the
        # default RDMA threshold); the final part can be smaller than that.
        self.part1 = os.urandom(5 * 1024 * 1024)
        self.part2 = os.urandom(1024)

    def test_s3_rdma_upload_part(self):
        create_rsp = self.client.create_multipart_upload(
            Bucket=self.bucket_name, Key=self.key
        )
        upload_id = create_rsp["UploadId"]

        try:
            parts = []
            for part_number, data in enumerate(
                [self.part1, self.part2], start=1
            ):
                ptr = CuDataPtr.from_buffer(data)
                with self.client.register_buffer(ptr):
                    rsp = self.client.upload_part_direct(
                        ptr,
                        Bucket=self.bucket_name,
                        Key=self.key,
                        UploadId=upload_id,
                        PartNumber=part_number,
                    )
                parts.append({"ETag": rsp["ETag"], "PartNumber": part_number})

            self.client.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=self.key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
        except Exception:
            self.client.abort_multipart_upload(
                Bucket=self.bucket_name, Key=self.key, UploadId=upload_id
            )
            raise

        response = self.client.get_object_tcp(
            Bucket=self.bucket_name, Key=self.key
        )
        self.assertEqual(self.part1 + self.part2, response["Body"].read())

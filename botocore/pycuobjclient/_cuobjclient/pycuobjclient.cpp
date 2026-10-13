// Copyright Cloudian, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You
// may not use this file except in compliance with the License. A copy of
// the License is located at
//
// http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
// ANY KIND, either express or implied. See the License for the specific
// language governing permissions and limitations under the License.

#include <pybind11/pybind11.h>

#include <cuobjclient.h>

#include <algorithm>
#include <cstring>
#include <dlfcn.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace py = pybind11;

struct PyCuPtr {
    const uintptr_t data_;
    const size_t size_;

    static PyCuPtr fromBuffer(py::buffer buf) {
        const py::buffer_info info = buf.request();
        return PyCuPtr{ .data_ = reinterpret_cast<uintptr_t>(info.ptr), .size_ = static_cast<size_t>(info.size)};
    }

    static PyCuPtr fromRaw(uintptr_t data, size_t size) {
        return PyCuPtr{ .data_ = data, .size_ = size};
    }

    void copyFromBuffer(py::buffer src) {
        const py::buffer_info info = src.request();
        const auto to_copy = std::min(size_, static_cast<size_t>(info.size));
        memcpy(reinterpret_cast<uint8_t*>(data_), info.ptr, to_copy);
    }

    void copyToBuffer(py::buffer dst, size_t dst_offset) {
        const py::buffer_info info = dst.request();
        if (dst_offset > info.size) {
            throw std::length_error("offset out of bounds");
        }
        const auto to_copy = std::min(size_, static_cast<size_t>(info.size - dst_offset));
        memcpy(reinterpret_cast<uint8_t*>(info.ptr) + dst_offset, reinterpret_cast<const uint8_t*>(data_), to_copy);
    }

    PyCuPtr slice(size_t offset, size_t len) {
        if (offset + len > size_) {
            throw std::length_error("slice out of bounds");
        }
        return PyCuPtr{ .data_ = data_ + offset, .size_ = len};
    }

    void* data() const {
        return reinterpret_cast<void*>(data_);
    }

    size_t size() const {
        return size_;
    }

    py::buffer_info get_buffer_info() {
        return py::buffer_info(data(), sizeof(uint8_t), py::format_descriptor<uint8_t>::format(), size_, true);
    }
};

class PyCuDataBuffer {
    std::unique_ptr<uint8_t[]> buf_;
    size_t size_;

    public:
        PyCuDataBuffer(size_t size) : buf_{new uint8_t[size]}, size_{size} {}

        static PyCuDataBuffer fromBytes(py::bytes buf) {
            std::string_view sv = buf;
            auto rc = PyCuDataBuffer(sv.size());
            memcpy(rc.buf_.get(), sv.data(), sv.size());
            return rc;
        }

        uint8_t* data() const {
            return buf_.get();
        }

        size_t size() const {
            return size_;
        }

        PyCuPtr dataPtr() const {
            return PyCuPtr::fromRaw(reinterpret_cast<uintptr_t>(buf_.get()), size_);
        }

        PyCuPtr slice(size_t offset, size_t len) {
            return dataPtr().slice(offset, len);
        }

        py::buffer_info get_buffer_info() {
            return py::buffer_info(buf_.get(), sizeof(uint8_t), py::format_descriptor<uint8_t>::format(), size_, true);
        }
};

// cuObjClient (declared in cuobjclient.h) is normally linked directly
// against libcuobjclient.so, which makes that library (and its own
// dependency on libcufile.so) a hard requirement just to *import* this
// extension - even for callers who only ever fall back to TCP. Instead we
// bind to it lazily via dlopen/dlsym, using the exact mangled symbol names
// for the handful of cuObjClient methods we call. This keeps the
// PyCuObjClient interface below unchanged; the only difference is that
// isConnected() (and everything else) report the client as unavailable
// instead of the whole module failing to import.
//
// This assumes the ABI (symbol names, object layout) of the cuObjClient
// class matches what botocore/pycuobjclient/_cuobjclient/pycuobjclient.cpp
// was compiled against - true for the NVIDIA cuObject SDK versions this was
// verified against, but a binary-incompatible SDK upgrade could in
// principle change either.
namespace cuobj_dl {

using Ctor = void (*)(void* self, CUObjIOOps&, cuObjProto_t);
using Dtor = void (*)(void* self);
using IsConnected = bool (*)(void* self);
using GetMemoryType = cuObjMemoryType_t (*)(const void* ptr);
using CuMemObjGetDescriptor = cuObjErr_t (*)(void* self, void* ptr, size_t size);
using CuMemObjPutDescriptor = cuObjErr_t (*)(void* self, void* ptr);
using CuMemObjGetRDMAToken = cuObjErr_t (*)(void* self, void* ptr, size_t size, size_t buffer_offset, cuObjOpType_t operation, char** desc_str_out);
using CuMemObjPutRDMAToken = cuObjErr_t (*)(void* self, char* desc_str);

struct Api {
    Ctor ctor;
    Dtor dtor;
    IsConnected isConnected;
    GetMemoryType getMemoryType;
    CuMemObjGetDescriptor getDescriptor;
    CuMemObjPutDescriptor putDescriptor;
    CuMemObjGetRDMAToken getRdmaToken;
    CuMemObjPutRDMAToken putRdmaToken;
};

template <typename Fn>
bool resolve(void* handle, const char* mangled_name, Fn& out) {
    out = reinterpret_cast<Fn>(dlsym(handle, mangled_name));
    return out != nullptr;
}

// Resolve and cache the cuObjClient entry points on first use. Returns
// nullptr if libcuobjclient.so (or a dependency, e.g. libcufile.so) can't be
// found, or if any expected symbol is missing from what was found.
const Api* load() {
    static const Api* api = []() -> const Api* {
        static const char* candidates[] = {
            "libcuobjclient.so",
            "libcuobjclient.so.1",
        };
        void* handle = nullptr;
        for (auto name : candidates) {
            handle = dlopen(name, RTLD_NOW | RTLD_GLOBAL);
            if (handle) {
                break;
            }
        }
        if (!handle) {
            return nullptr;
        }

        static Api a{};
        bool ok = true;
        ok &= resolve(handle, "_ZN11cuObjClientC1ER10CUObjIOOps15cuObjProto_enum", a.ctor);
        ok &= resolve(handle, "_ZN11cuObjClientD1Ev", a.dtor);
        ok &= resolve(handle, "_ZN11cuObjClient11isConnectedEv", a.isConnected);
        ok &= resolve(handle, "_ZN11cuObjClient13getMemoryTypeEPKv", a.getMemoryType);
        ok &= resolve(handle, "_ZN11cuObjClient21cuMemObjGetDescriptorEPvm", a.getDescriptor);
        ok &= resolve(handle, "_ZN11cuObjClient21cuMemObjPutDescriptorEPv", a.putDescriptor);
        ok &= resolve(handle, "_ZN11cuObjClient20cuMemObjGetRDMATokenEPvmm16cuObjOpType_enumPPc", a.getRdmaToken);
        ok &= resolve(handle, "_ZN11cuObjClient20cuMemObjPutRDMATokenEPc", a.putRdmaToken);
        if (!ok) {
            return nullptr;
        }
        return &a;
    }();
    return api;
}

}  // namespace cuobj_dl

// Forward reference
class PyCuObjClient;

// An RDMA token allocated by cuObjClient. Returns the token to the library
// on destruction rather than requiring callers to remember to release it -
// mirrors the RdmaToken RAII wrapper in the AWS SDK RDMA fork
// (cuobj-aws-s3-sdk-cpp/src/CuObjClient.h).
class RdmaToken {
    friend class PyCuObjClient;
    RdmaToken(PyCuObjClient* client, char* token) : client_(client), token_(token) {}

public:
    ~RdmaToken();

    RdmaToken(const RdmaToken&) = delete;
    RdmaToken& operator=(const RdmaToken&) = delete;

    RdmaToken(RdmaToken&& other) noexcept : client_(other.client_), token_(other.token_) {
        other.token_ = nullptr;
    }

    RdmaToken& operator=(RdmaToken&& other) noexcept;

    bool empty() const { return token_ == nullptr; }

    std::string get() const { return token_ ? std::string(token_) : std::string(); }

private:
    PyCuObjClient* client_;
    char* token_;
};

class PyCuObjClient {
    friend class RdmaToken;
    static CUObjIOOps ops_;

    // Resolved once at construction time - nullptr means libcuobjclient.so
    // (or a dependency, e.g. libcufile.so) couldn't be found, i.e. RDMA is
    // unavailable on this system.
    const cuobj_dl::Api* api_;

    // Raw storage for a cuObjClient, constructed in place via api_'s
    // dlsym'd constructor/destructor rather than a normal member so that
    // this module doesn't need libcuobjclient.so at load time. Only valid
    // to use client() when api_ is non-null.
    alignas(alignof(cuObjClient)) unsigned char storage_[sizeof(cuObjClient)];

    cuObjClient* client() {
        return reinterpret_cast<cuObjClient*>(storage_);
    }

    void ensureAvailable() {
        if (!api_) {
            throw std::runtime_error(
                "libcuobjclient.so is not available - RDMA is disabled"
            );
        }
    }

    public:
        PyCuObjClient() : api_(cuobj_dl::load()) {
            if (api_) {
                api_->ctor(storage_, ops_, CUOBJ_PROTO_RDMA_DC_V1);
            }
        }

        ~PyCuObjClient() {
            if (api_) {
                api_->dtor(storage_);
            }
        }

        // Check if client is connected (to what I'm not quite sure!)
        // Returns false (rather than raising) when libcuobjclient.so isn't
        // available, so Python-level RDMA-availability checks degrade the
        // same way as "not connected to an RDMA daemon".
        bool isConnected(void) {
            return api_ && api_->isConnected(client());
        }

        ssize_t registerBufDescriptor(PyCuPtr data) {
            ensureAvailable();
            auto rc = api_->getDescriptor(client(), data.data(), data.size());
            // std::cout << "registerBufDescriptor: " << data.data_ << " " << data.size_ << " rc=" << rc << std::endl;
            return rc;
        }

        ssize_t releaseBufDescriptor(PyCuPtr data) {
            ensureAvailable();
            auto rc = api_->putDescriptor(client(), data.data());
            // std::cout << "releaseBufDescriptor: " << data.data_ << " " << data.size_ << " rc=" << rc << std::endl;
            return rc;
        }

        // Check the ptr to see if it points to system memory.
        // If the shared library isn't available, we assume system memory.
        static bool isSystemMemory(PyCuPtr data) {
            const cuobj_dl::Api* api = cuobj_dl::load();
            return !api || api->getMemoryType(data.data()) == CUOBJ_MEMORY_SYSTEM;
        }

        // Get an RDMA token for a get operation on a registered buffer
        // region, for embedding (via str()) directly in the S3
        // x-amz-rdma-token header - no callback/round-trip through
        // cuObjGet needed.
        //
        // The token is only released back to the library once the returned
        // RdmaToken is destroyed, so the Python caller must keep it alive
        // (e.g. in a local variable) for as long as the S3 request that
        // embeds it is in flight - the server performs the actual RDMA
        // transfer against this token while handling that request, so
        // releasing it any earlier would let the token be reused/invalidated
        // out from under an in-progress transfer.
        RdmaToken getRdmaGetToken(PyCuPtr data, size_t bufferOffset = 0) {
            return getRdmaToken(data, bufferOffset, CUOBJ_GET);
        }

        // Same as getRdmaGetToken, but for a put/upload-part operation.
        RdmaToken getRdmaPutToken(PyCuPtr data, size_t bufferOffset = 0) {
            return getRdmaToken(data, bufferOffset, CUOBJ_PUT);
        }

    private:
        RdmaToken getRdmaToken(PyCuPtr data, size_t bufferOffset, cuObjOpType_t operation) {
            ensureAvailable();
            char* desc = nullptr;
            auto rc = api_->getRdmaToken(client(), data.data(), data.size(), bufferOffset, operation, &desc);
            if (rc != CU_OBJ_SUCCESS || !desc) {
                throw std::runtime_error("cuMemObjGetRDMAToken failed");
            }
            return RdmaToken(this, desc);
        }

        // Only RdmaToken's destructor/move-assignment call this, to return a
        // token to the library once it goes out of scope.
        void releaseRdmaToken(char* desc) {
            api_->putRdmaToken(client(), desc);
        }

        // cuObjClient's constructor requires a CUObjIOOps callback table,
        // but we do not use cuObjGet/cuObjPut (see getRdmaGetToken /
        // getRdmaPutToken above) so these are never actually invoked.
        static ssize_t unusedGetCallback(const void*, char*, size_t, loff_t, const cufileRDMAInfo_t*) {
            return CU_OBJ_FAIL;
        }

        static ssize_t unusedPutCallback(const void*, const char*, size_t, loff_t, const cufileRDMAInfo_t*) {
            return CU_OBJ_FAIL;
        }
};


CUObjIOOps PyCuObjClient::ops_ = {
    PyCuObjClient::unusedGetCallback,
    PyCuObjClient::unusedPutCallback
};

// Defined out-of-line, after PyCuObjClient, since these need it complete.
RdmaToken::~RdmaToken() {
    if (token_) {
        client_->releaseRdmaToken(token_);
    }
}

RdmaToken& RdmaToken::operator=(RdmaToken&& other) noexcept {
    if (this != &other) {
        if (token_) {
            client_->releaseRdmaToken(token_);
        }
        client_ = other.client_;
        token_ = other.token_;
        other.token_ = nullptr;
    }
    return *this;
}


class PyCuBufferPool {
    std::mutex m_;
    PyCuObjClient* const client_;
    const size_t maxPoolSize_;
    const size_t bufferSize_;
    std::vector<std::shared_ptr<PyCuDataBuffer>> pool_;
    size_t nBuffers_;

    private:
        std::shared_ptr<PyCuDataBuffer> allocate_buffer() {
            auto buf = std::make_shared<PyCuDataBuffer>(bufferSize_);
            if (client_) {
                auto rc = client_->registerBufDescriptor(buf->dataPtr());
                if (rc < 0) {
                    throw std::runtime_error("Failed to register pooled buffer");
                }
            }
            return buf;
        }

        void free_buffer(std::shared_ptr<PyCuDataBuffer>&& buf) {
            if (client_) {
                auto rc = client_->releaseBufDescriptor(buf->dataPtr());
                if (rc < 0) {
                    throw std::runtime_error("Failed to release pooled buffer");
                }
            }
        }


    public:
        PyCuBufferPool(PyCuObjClient* client, size_t nBuffers, size_t bufferSize) : client_{client}, maxPoolSize_{nBuffers}, bufferSize_{bufferSize}, nBuffers_{nBuffers} {
            for (size_t i = 0; i < nBuffers; i++) {
                pool_.push_back(allocate_buffer());
            }
        }

        ~PyCuBufferPool() {
            for (size_t i = 0; i < nBuffers_; i++) {
                free_buffer(std::move(pool_[i]));
            }
        }

        size_t buffer_size() const {
            return bufferSize_;
        }

        size_t capacity() const {
            return maxPoolSize_;
        }

        size_t available() const {
            return nBuffers_;
        }

        std::shared_ptr<PyCuDataBuffer> get_buffer() {
            {
                std::lock_guard lock(m_);
                if (nBuffers_) {
                    nBuffers_ -= 1;
                    return std::move(pool_[nBuffers_]);
                }
            }
            // No buffers in the pool - we'll allocate one instead
            return allocate_buffer();
        }

        void put_buffer(std::shared_ptr<PyCuDataBuffer> buf) {
            {
                std::lock_guard lock(m_);
                if (nBuffers_ < maxPoolSize_) {
                    pool_[nBuffers_] = std::move(buf);
                    nBuffers_ += 1;
                    return;
                }
            }
            // Pool is full, destroy buffer
            free_buffer(std::move(buf));
        }
};

// Create an uninitialized bytes object, which will be filled with read data
py::bytes createReadBuffer(size_t len) {
    auto buf = PyBytes_FromStringAndSize(nullptr, len);
    if (!buf) {
        throw std::bad_alloc();
    }

    return py::reinterpret_steal<py::bytes>(buf);
}

PYBIND11_MODULE(_cuobjclient, m) {
    m.doc() = "python bindings to Nvidia CuObj client library";

    m.def("cu_create_read_buffer", &createReadBuffer, "Creates an uninitialized bytes object that will be filled in by readinto()");

    py::class_<PyCuDataBuffer, std::shared_ptr<PyCuDataBuffer>>(m, "CuDataBuffer", py::buffer_protocol())
        .def(py::init<size_t>())
        .def("data_ptr", &PyCuDataBuffer::dataPtr)
        .def("slice", &PyCuDataBuffer::slice)
        .def_static("from_bytes", &PyCuDataBuffer::fromBytes)
        .def_buffer(&PyCuDataBuffer::get_buffer_info);

    py::class_<PyCuPtr>(m, "CuDataPtr", py::buffer_protocol())
        .def_static("from_buffer", &PyCuPtr::fromBuffer)
        .def_static("from_raw", &PyCuPtr::fromRaw)
        .def("slice", &PyCuPtr::slice)
        .def("copy_from_buffer", &PyCuPtr::copyFromBuffer)
        .def("copy_to_buffer", &PyCuPtr::copyToBuffer)
        .def("size", &PyCuPtr::size)
        .def_buffer(&PyCuPtr::get_buffer_info);

    // No py::init<>() - only ever handed to Python by CuObjClient's
    // get_rdma_get_token/get_rdma_put_token below. Holding this object keeps
    // the underlying RDMA token allocated; it's returned to the library once
    // Python drops the last reference to it (garbage collection, del, or the
    // holding variable going out of scope).
    py::class_<RdmaToken>(m, "RdmaToken")
        .def("empty", &RdmaToken::empty)
        .def("__str__", &RdmaToken::get)
        .def("__repr__", [](const RdmaToken& t) {
            return t.empty() ? std::string("<RdmaToken empty>")
                              : "<RdmaToken>";
        });

    py::class_<PyCuObjClient>(m, "CuObjClient")
        .def(py::init<>())
        .def("is_connected", &PyCuObjClient::isConnected)
        .def("register_descriptor", &PyCuObjClient::registerBufDescriptor)
        .def("release_descriptor", &PyCuObjClient::releaseBufDescriptor)
        .def_static("is_system_memory", &PyCuObjClient::isSystemMemory)
        .def("get_rdma_get_token", &PyCuObjClient::getRdmaGetToken, py::arg("data"), py::arg("buffer_offset") = 0)
        .def("get_rdma_put_token", &PyCuObjClient::getRdmaPutToken, py::arg("data"), py::arg("buffer_offset") = 0);

    py::class_<PyCuBufferPool>(m, "CuBufferPool", py::buffer_protocol())
        // PyCuBufferPool only holds a raw, non-owning pointer to the
        // PyCuObjClient it registers buffers with, and dereferences it
        // again on destruction (to release descriptors) - so the
        // CuObjClient argument must be kept alive for at least as long as
        // this CuBufferPool, or it can end up dangling if Python garbage
        // collects the client first (e.g. __dict__ clearing in insertion
        // order during client teardown).
        .def(py::init<PyCuObjClient*, size_t, size_t>(), py::keep_alive<1, 2>())
        .def("buffer_size", &PyCuBufferPool::buffer_size)
        .def("capacity", &PyCuBufferPool::capacity)
        .def("available", &PyCuBufferPool::available)
        .def("get_buffer", &PyCuBufferPool::get_buffer)
        .def("put_buffer", &PyCuBufferPool::put_buffer);
}

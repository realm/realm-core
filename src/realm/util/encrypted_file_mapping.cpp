/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/encrypted_file_mapping.hpp>

#include <realm/util/backtrace.hpp>
#include <realm/util/file_mapper.hpp>

#include <sstream>

#if REALM_ENABLE_ENCRYPTION
#include <realm/util/aes_cryptor.hpp>
#include <realm/util/errno.hpp>
#include <realm/util/sha_crypto.hpp>
#include <realm/util/terminate.hpp>
#include <realm/utilities.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>
#include <thread>

#ifdef REALM_DEBUG
#include <cstdio>
#endif

#if defined(_WIN32)
#include <Windows.h>
#include <bcrypt.h>
#pragma comment(lib, "bcrypt.lib")
#else
#include <sys/mman.h>
#include <unistd.h>
#endif

namespace realm::util {
// When Realm's file encryption was originally designed, we had the constraint
// that all encryption and decryption had to happen in aligned system page size
// sized blocks due to the use of signal handlers to lazily decrypt data and
// track where writes occurrs. This is no longer the case, but may still help
// explain why the file layout looks the way it does.
//
// Encryption is performed on 4096 byte data pages. Each group of 64 data pages
// is arranged into a "block", which has a 4096 byte header containing the IVs
// and HMACs for the following pages. Each page has *two* IVs and HMACs stored.
// iv2/hmac2 contain the values which were last used to successfully decrypt
// the page, while iv1/hmac1 is the values which were used to last encrypt the
// page.
//
// Writing new encrypted data has the following steps:
//
// 1. Copy iv1/hmac1 to iv2/hmac2 in the IVTable
// 2. Increment iv1
// 3. Encrypt the page in memory
// 4. Compute the hmac for the new encrypted data.
// 5. If the hmac matches the previous hmac, goto 2 (this will not ever actually happen)
// 6. Write the new IVTable for the page.
// 7. fsync() (or F_BARRIERFSYNC on Apple)
// 8. Write the new encrypted data
//
// If we are interrupted before #6, no i/o has happened and the data on disk is
// fine. If we are interrupted between #6 and #8, then when we next try to read
// the page the hmac check using hmac1 will fail, but the check using hmac2
// will succeed and we will be able to read the old data. We then copy
// iv2/hmac2 back to the active fields and continue as normal.
//
// This scheme breaks if we have a partial write of the 4k page. This is
// impossible with SSDs, which can only write in their atomic block size, and
// it would be extremely unusual for that to be smaller than 4k. It may be a
// problem when running on HDDs, though.
//
// Reading from an encrypted file is done by creating a mapping and then
// calling `read_barrier(addr, size)` to mark the section of the mapping which
// needs to be populated. This decrypts each of the pages which cover that
// range and places the plaintext into memory. If any of the pages were already
// decrypted, this is a no-op that skips reading anything and just assumes that
// the data was up-to-date.
//
// Writing is done with `read_barrier(addr, size, true)` before performing any
// writes to mark the range as writeable, and then `write_barrier(addr, size)`
// to mark bytes which were actually written to. `write_barrier()` eagerly
// copies all of the written bytes to any other active mappings on the same
// file which have those pages decrypted in memory. This is spooky
// threading-wise, and is only made safe by Realm's MVCC semantics - if we're
// writing to a section of the file we know that no one can be legally reading
// those exact bytes, and we must be writing to different bytes in the same
// page. This copying makes it so that we never have to recheck the disk; once
// we have read and decrypted a page for a mapping, that page is forevermore
// valid and up-to-date.
//
// All dirty data is kept buffered in memory until `flush()` is called.
//
// In multi-process scenarios (or just multiple File instances for a single
// file in a single process, which doesn't happen when using the public API
// normally), eagerly keeping decrypted pages up to date is impossible, and we
// sometimes need to recheck the disk. Here we once again take advantage of
// Realm being MVCC with discrete points where we may need to see newer
// versions of the data on disk. When the reader view is updated, if there have
// been any external writes to the file SlabAlloc calls
// `mark_pages_for_iv_check()`, which puts all up-to-date pages into a
// potentially-stale state. The next time each page is accessed, we reread the
// IVTable for that page. If it's the same as the IVTable for the plaintext we
// have in memory then the page is marked as being up-to-date, and if it's
// different we reread the page.
//
// Another source of complexity in multiprocess scenarios is that while we
// assume that the actual i/o is atomic in 4k chunks, writing to the in-memory
// buffers is distinctly not atomic. One process reading from a memory mapping
// while another process is writing to that position in the file can see
// incomplete writes. Rather than doing page-level locking, we assume that this
// will be very rare and perform optimistic unlocked reads. If decryption fails
// and we are in a potentially-multiprocess scenario we retry the read several
// times before reporting an error.

struct IVTable {
    uint32_t iv1 = 0;
    std::array<uint8_t, 28> hmac1 = {};
    uint32_t iv2 = 0;
    std::array<uint8_t, 28> hmac2 = {};
    bool operator==(const IVTable& other) const
    {
        return iv1 == other.iv1 && iv2 == other.iv2 && hmac1 == other.hmac1 && hmac2 == other.hmac2;
    }
    bool operator!=(const IVTable& other) const
    {
        return !(*this == other);
    }
};
// We read this via memcpy and need it to be packed
static_assert(sizeof(IVTable) == 64);

namespace {
constexpr uint8_t aes_block_size = 16;
constexpr uint16_t encryption_page_size = 4096;
constexpr uint8_t metadata_size = sizeof(IVTable);
constexpr uint8_t pages_per_block = encryption_page_size / metadata_size;
static_assert(metadata_size == 64,
              "changing the size of the metadata breaks compatibility with existing Realm files");

using SizeType = File::SizeType;

template <typename To, typename From>
To checked_cast(From from)
{
    To to;
    if (REALM_UNLIKELY(int_cast_with_overflow_detect(from, to))) {
        throw MaximumFileSizeExceeded(util::format("File size %1 is larger than can be represented", from));
    }
    return to;
}

// Overflows when converting from file positions (always 64-bits) to size_t
// (sometimes 32-bits) should all be caught by set_file_size()
template <typename To, typename From>
constexpr To assert_cast(From from)
{
    REALM_ASSERT_DEBUG(!int_cast_has_overflow<To>(from));
    return static_cast<To>(from);
}

// Index of page which contains `data_pos`
constexpr size_t page_index(SizeType data_pos) noexcept
{
    SizeType index = data_pos / encryption_page_size;
    return assert_cast<size_t>(index);
}

// Number of pages required to store `size` bytes
constexpr size_t page_count(SizeType size) noexcept
{
    return assert_cast<size_t>((size + encryption_page_size - 1) / encryption_page_size);
}

// Index of the metadata block which contains `data_pos`
constexpr size_t block_index(SizeType data_pos) noexcept
{
    return page_index(data_pos) / pages_per_block;
}

// Number of metadata blocks required to store `size` bytes
constexpr size_t block_count(SizeType data_size) noexcept
{
    return (page_count(data_size) + pages_per_block - 1) / pages_per_block;
}

// map an offset in the data to the actual location in the file
SizeType data_pos_to_file_pos(SizeType data_pos)
{
    REALM_ASSERT(data_pos >= 0);
    return data_pos + (block_index(data_pos) + 1) * encryption_page_size;
}

// map a location in the file to the offset in the data
SizeType file_pos_to_data_pos(SizeType file_pos)
{
    REALM_ASSERT(file_pos >= 0);
    const size_t metadata_page_count = (page_index(file_pos) + pages_per_block) / (pages_per_block + 1);
    return file_pos - metadata_page_count * encryption_page_size;
}

// get the location of the IVTable for the given data (not file) position
SizeType iv_table_pos(SizeType data_pos)
{
    REALM_ASSERT(data_pos >= 0);
    const size_t index = page_index(data_pos);
    const size_t metadata_block = block_index(data_pos);
    const size_t metadata_index = index & (pages_per_block - 1);
    return metadata_block * (pages_per_block + 1) * encryption_page_size + metadata_index * metadata_size;
}

// get the file location of the IVTable block for the given data (not file) position
SizeType iv_table_block_pos(SizeType data_pos)
{
    REALM_ASSERT(data_pos >= 0);
    return block_index(data_pos) * (pages_per_block + 1) * encryption_page_size;
}

constexpr size_t iv_table_size(SizeType data_pos)
{
    return block_count(data_pos) * pages_per_block;
}

// not actually checked any more
size_t check_read(FileDesc fd, SizeType pos, void* dst)
{
    return File::read_static(fd, pos, static_cast<char*>(dst), encryption_page_size);
}

// first block is iv data, second page is data
static_assert(c_min_encrypted_file_size == 2 * encryption_page_size,
              "chaging the block size breaks encrypted file portability");

template <class T, size_t N, std::size_t... I>
constexpr std::array<T, N> to_array_impl(const T* ptr, std::index_sequence<I...>)
{
    return {{ptr[I]...}};
}
template <class T, size_t N>
constexpr auto to_array(const T* ptr)
{
    return to_array_impl<T, N>(ptr, std::make_index_sequence<N>{});
}

void memcpy_if_changed(void* dst, const void* src, size_t n)
{
#if REALM_SANITIZE_THREAD
    // Because our copying is page-level granularity, we have some benign races
    // where the byte ranges in each page that weren't modified get overwritten
    // with the same values as they already had. TSan correctly reports this as
    // a data race, so when using TSan do (much slower) byte-level checking for
    // modifications and only write the ones which changed. Unlike suppressing
    // the warning entirely, this will still produce tsan errors if we actually
    // change any bytes that another thread is reading.
    auto dst_2 = static_cast<char*>(dst);
    auto src_2 = static_cast<const char*>(src);
    for (size_t i = 0; i < n; ++i) {
        if (dst_2[i] != src_2[i])
            dst_2[i] = src_2[i];
    }
#else
    memcpy(dst, src, n);
#endif
}

} // anonymous namespace

AESCryptor::AESCryptor(const char* key)
    : m_key(to_array<uint8_t, 64>(reinterpret_cast<const uint8_t*>(key)))
    , m_rw_buffer(new char[encryption_page_size])
    , m_dst_buffer(new char[encryption_page_size])
{
#if REALM_PLATFORM_APPLE
    // A random iv is passed to CCCryptorReset. This iv is *not used* by Realm; we set it manually prior to
    // each call to BCryptEncrypt() and BCryptDecrypt(). We pass this random iv as an attempt to
    // suppress a false encryption security warning from the IBM Bluemix Security Analyzer (PR[#2911])
    unsigned char u_iv[kCCKeySizeAES256];
    arc4random_buf(u_iv, kCCKeySizeAES256);
    void* iv = u_iv;
    CCCryptorCreate(kCCEncrypt, kCCAlgorithmAES, 0 /* options */, key, kCCKeySizeAES256, iv, &m_encr);
    CCCryptorCreate(kCCDecrypt, kCCAlgorithmAES, 0 /* options */, key, kCCKeySizeAES256, iv, &m_decr);
#elif defined(_WIN32)
    BCRYPT_ALG_HANDLE hAesAlg = NULL;
    int ret;
    ret = BCryptOpenAlgorithmProvider(&hAesAlg, BCRYPT_AES_ALGORITHM, NULL, 0);
    REALM_ASSERT_RELEASE_EX(ret == 0 && "BCryptOpenAlgorithmProvider()", ret);

    ret = BCryptSetProperty(hAesAlg, BCRYPT_CHAINING_MODE, (PBYTE)BCRYPT_CHAIN_MODE_CBC,
                            sizeof(BCRYPT_CHAIN_MODE_CBC), 0);
    REALM_ASSERT_RELEASE_EX(ret == 0 && "BCryptSetProperty()", ret);

    ret = BCryptGenerateSymmetricKey(hAesAlg, &m_aes_key_handle, nullptr, 0, (PBYTE)key, 32, 0);
    REALM_ASSERT_RELEASE_EX(ret == 0 && "BCryptGenerateSymmetricKey()", ret);
#else
    m_ctx = EVP_CIPHER_CTX_new();
    if (!m_ctx)
        handle_error();
#endif
}

AESCryptor::~AESCryptor() noexcept
{
#if REALM_PLATFORM_APPLE
    CCCryptorRelease(m_encr);
    CCCryptorRelease(m_decr);
#elif defined(_WIN32)
#else
    EVP_CIPHER_CTX_cleanup(m_ctx);
    EVP_CIPHER_CTX_free(m_ctx);
#endif
}

void AESCryptor::handle_error()
{
    throw std::runtime_error("Error occurred in encryption layer");
}

void AESCryptor::set_data_size(SizeType new_data_size)
{
    REALM_ASSERT(new_data_size >= 0);
    m_iv_buffer.reserve(iv_table_size(new_data_size));
    m_iv_buffer_cache.reserve(m_iv_buffer.capacity());
    m_iv_blocks_read.resize(m_iv_buffer.capacity() / 64);
}

IVTable& AESCryptor::get_iv_table(FileDesc fd, SizeType data_pos, IVLookupMode mode) noexcept
{
    size_t idx = page_index(data_pos);
    REALM_ASSERT(idx < m_iv_buffer.capacity()); // required space should have been preallocated
    if (mode != IVLookupMode::UseCache || idx >= m_iv_buffer.size() || !m_iv_blocks_read[block_index(data_pos)]) {
        read_iv_block(fd, data_pos);
    }
    m_iv_buffer_cache[idx] = m_iv_buffer[idx];
    return m_iv_buffer[idx];
}

// We always read an entire block of IVTables at a time rather than just the
// one we need as it's likely to take about the same amount of time up front
// and greatly reduce the total number of read calls we have to make
void AESCryptor::read_iv_block(FileDesc fd, SizeType data_pos)
{
    size_t idx = block_index(data_pos) * pages_per_block;
    if (idx + pages_per_block > m_iv_buffer.size()) {
        m_iv_buffer.resize(idx + pages_per_block);
        m_iv_buffer_cache.resize(m_iv_buffer.size());
    }
    SizeType iv_pos = iv_table_block_pos(data_pos);
    check_read(fd, iv_pos, &m_iv_buffer[idx]);
    m_iv_blocks_read[block_index(data_pos)] = true;
}

void AESCryptor::calculate_hmac(Hmac& hmac) const
{
    hmac_sha224(Span(reinterpret_cast<const uint8_t*>(m_rw_buffer.get()), encryption_page_size), hmac,
                Span(m_key).sub_span<32>());
}

bool AESCryptor::constant_time_equals(const Hmac& a, const Hmac& b) const
{
    // Constant-time memcmp to avoid timing attacks
    uint8_t result = 0;
    for (size_t i = 0; i < a.size(); ++i)
        result |= a[i] ^ b[i];
    return result == 0;
}

bool AESCryptor::refresh_iv(FileDesc fd, size_t page_ndx)
{
    REALM_ASSERT(page_ndx < m_iv_buffer.capacity());
    if (page_ndx >= m_iv_buffer.size() || !m_iv_blocks_read[page_ndx / pages_per_block]) {
        read_iv_block(fd, SizeType(page_ndx) * encryption_page_size);
    }

    if (m_iv_buffer[page_ndx] != m_iv_buffer_cache[page_ndx]) {
        m_iv_buffer_cache[page_ndx] = m_iv_buffer[page_ndx];
        return true;
    }
    return false;
}

void AESCryptor::invalidate_ivs() noexcept
{
    m_iv_blocks_read.assign(m_iv_blocks_read.size(), false);
}

AESCryptor::ReadResult AESCryptor::read(FileDesc fd, SizeType pos, char* dst, WriteObserver* observer)
{
    uint32_t iv = 0;
    Hmac hmac;
    // We're in a single-process scenario (or other processes are only reading),
    // so we can trust our in-memory caches and never need to retry
    if (!observer || observer->no_concurrent_writer_seen()) {
        return attempt_read(fd, pos, dst, IVLookupMode::UseCache, iv, hmac);
    }

    // There's another process which might be trying to write to the file while
    // we're reading from it, which means that we might see invalid data due to
    // data races. When this happens we need to retry the read, and only throw
    // an error if the data either hasn't changed after the timeout has expired
    // or if we're in a reader starvation scenario where the writer is producing
    // new data faster than we can consume it.
    size_t retry_count = 0;
    std::pair<uint32_t, Hmac> last_iv_and_data_hash;
    auto retry_start_time = std::chrono::steady_clock::now();
    size_t num_identical_reads = 1;
    ReadResult result = ReadResult::Success;
    while (retry_count <= 5 || (retry_count - num_identical_reads > 1 && retry_count < 20)) {
        result =
            attempt_read(fd, pos, dst, retry_count == 0 ? IVLookupMode::UseCache : IVLookupMode::Refetch, iv, hmac);
        switch (result) {
            case ReadResult::Success:
            case ReadResult::Eof:
            case ReadResult::Uninitialized:
                // Consistent and valid states that may or may not actually have data
                return result;
            case ReadResult::InterruptedFirstWrite:
            case ReadResult::StaleHmac:
            case ReadResult::Failed:
                // Inconsistent states which may change if we retry
                break;
        }

        // Check if we've timed out, but always retry at least once in case
        // we got suspended while another process was writing or something
        constexpr auto max_retry_period = std::chrono::seconds(5);
        auto elapsed = std::chrono::steady_clock::now() - retry_start_time;
        if (retry_count > 0 && elapsed > max_retry_period) {
            auto str = util::format("unable to decrypt after %1 seconds (retry_count=%2)",
                                    std::chrono::duration_cast<std::chrono::seconds>(elapsed).count(), retry_count);
            // std::cerr << std::endl << "*Timeout: " << str << std::endl;
            throw DecryptionFailed(str);
        }

        // don't wait on the first retry as we want to optimize the case where the first read
        // from the iv table cache didn't validate and we are fetching the iv block from disk for the first time
        std::pair cur_iv_and_data_hash(iv, hmac);
        if (retry_count != 0) {
            if (last_iv_and_data_hash == cur_iv_and_data_hash) {
                ++num_identical_reads;
            }
            // don't retry right away if there are potentially other external writers
            std::this_thread::yield();
        }
        last_iv_and_data_hash = cur_iv_and_data_hash;
        ++retry_count;

        if (observer->no_concurrent_writer_seen())
            break;
    }

    return result;
}

AESCryptor::ReadResult AESCryptor::attempt_read(FileDesc fd, SizeType pos, char* dst, IVLookupMode iv_mode,
                                                uint32_t& iv_out, Hmac& hmac)
{
    IVTable& iv = get_iv_table(fd, pos, iv_mode);
    iv_out = iv.iv1;
    if (iv.iv1 == 0) {
        hmac.fill(0);
        return ReadResult::Uninitialized;
    }

    size_t actual = check_read(fd, data_pos_to_file_pos(pos), m_rw_buffer.get());
    if (actual < encryption_page_size) {
        return ReadResult::Eof;
    }

    calculate_hmac(hmac);
    if (!constant_time_equals(hmac, iv.hmac1)) {
        // Either the DB is corrupted or we were interrupted between writing the
        // new IV and writing the data
        if (iv.iv2 == 0) {
            return ReadResult::InterruptedFirstWrite;
        }

        if (constant_time_equals(hmac, iv.hmac2)) {
            // Un-bump the IV since the write with the bumped IV never actually
            // happened
            memcpy(&iv.iv1, &iv.iv2, 32);
        }
        else {
            // If the file has been shrunk and then re-expanded, we may have
            // old hmacs that don't go with this data. ftruncate() is
            // required to fill any added space with zeroes, so assume that's
            // what happened if the buffer is all zeroes
            bool all_zero = std::all_of(&m_rw_buffer[0], &m_rw_buffer[actual], [](char c) {
                return c == 0;
            });
            if (all_zero)
                return ReadResult::StaleHmac;
            return ReadResult::Failed;
        }
    }

    // We may expect some address ranges of the destination buffer of
    // AESCryptor::read() to stay unmodified, i.e. being overwritten with
    // the same bytes as already present, and may have read-access to these
    // from other threads while decryption is taking place.
    //
    // However, some implementations of AES_cbc_encrypt(), in particular
    // OpenSSL, will put garbled bytes as an intermediate step during the
    // operation which will lead to incorrect data being read by other
    // readers concurrently accessing that page. Incorrect data leads to
    // crashes.
    //
    // We therefore decrypt to a temporary buffer first and then copy the
    // completely decrypted data after.
    crypt(mode_Decrypt, pos, m_dst_buffer.get(), m_rw_buffer.get(), reinterpret_cast<const char*>(&iv.iv1));
    memcpy_if_changed(dst, m_dst_buffer.get(), encryption_page_size);
    return ReadResult::Success;
}

void AESCryptor::try_read_block(FileDesc fd, SizeType pos, char* dst) noexcept
{
    size_t bytes_read = check_read(fd, data_pos_to_file_pos(pos), m_rw_buffer.get());

    if (bytes_read == 0) {
        std::cerr << "Read failed: 0x" << std::hex << pos << std::endl;
        memset(dst, 0x55, encryption_page_size);
        return;
    }

    IVTable& iv = get_iv_table(fd, pos, IVLookupMode::Refetch);
    if (iv.iv1 == 0) {
        std::cerr << "Block never written: 0x" << std::hex << pos << std::endl;
        memset(dst, 0xAA, encryption_page_size);
        return;
    }

    Hmac hmac;
    calculate_hmac(hmac);
    if (!constant_time_equals(hmac, iv.hmac1)) {
        if (iv.iv2 == 0) {
            std::cerr << "First write interrupted: 0x" << std::hex << pos << std::endl;
        }

        if (constant_time_equals(hmac, iv.hmac2)) {
            std::cerr << "Restore old IV: 0x" << std::hex << pos << std::endl;
            memcpy(&iv.iv1, &iv.iv2, 32);
        }
        else {
            std::cerr << "Checksum failed: 0x" << std::hex << pos << std::endl;
        }
    }
    crypt(mode_Decrypt, pos, dst, m_rw_buffer.get(), reinterpret_cast<const char*>(&iv.iv1));
}

void AESCryptor::write(FileDesc fd, SizeType pos, const char* src, WriteMarker* marker) noexcept
{
    IVTable& iv = get_iv_table(fd, pos);

    memcpy(&iv.iv2, &iv.iv1, 32); // this is also copying the hmac
    do {
        ++iv.iv1;
        // 0 is reserved for never-been-used, so bump if we just wrapped around
        if (iv.iv1 == 0)
            ++iv.iv1;

        crypt(mode_Encrypt, pos, m_rw_buffer.get(), src, reinterpret_cast<const char*>(&iv.iv1));
        hmac_sha224(Span(reinterpret_cast<uint8_t*>(m_rw_buffer.get()), encryption_page_size), iv.hmac1,
                    Span(m_key).sub_span<32>());
        // In the extremely unlikely case that both the old and new versions have
        // the same hash we won't know which IV to use, so bump the IV until
        // they're different.
    } while (REALM_UNLIKELY(iv.hmac1 == iv.hmac2));

    if (marker)
        marker->mark(pos);
    File::write_static(fd, iv_table_pos(pos), reinterpret_cast<const char*>(&iv), sizeof(iv));
    // FIXME: doesn't this need a barrier? The IV table is very likely to
    // make it to disk first due to being issued first and being earlier in
    // the file, but not guaranteed
    File::write_static(fd, data_pos_to_file_pos(pos), m_rw_buffer.get(), encryption_page_size);
    if (marker)
        marker->unmark();
    m_iv_buffer_cache[page_index(pos)] = iv;
}

void AESCryptor::crypt(EncryptionMode mode, SizeType pos, char* dst, const char* src, const char* stored_iv) noexcept
{
    uint8_t iv[aes_block_size] = {0};
    memcpy(iv, stored_iv, 4);
    memcpy(iv + 4, &pos, sizeof(pos));

#if REALM_PLATFORM_APPLE
    CCCryptorRef cryptor = mode == mode_Encrypt ? m_encr : m_decr;
    CCCryptorReset(cryptor, iv);

    size_t bytesEncrypted = 0;
    CCCryptorStatus err =
        CCCryptorUpdate(cryptor, src, encryption_page_size, dst, encryption_page_size, &bytesEncrypted);
    REALM_ASSERT(err == kCCSuccess);
    REALM_ASSERT(bytesEncrypted == encryption_page_size);
#elif defined(_WIN32)
    ULONG cbData;
    int i;

    if (mode == mode_Encrypt) {
        i = BCryptEncrypt(m_aes_key_handle, (PUCHAR)src, encryption_page_size, nullptr, (PUCHAR)iv, sizeof(iv),
                          (PUCHAR)dst, encryption_page_size, &cbData, 0);
        REALM_ASSERT_RELEASE_EX(i == 0 && "BCryptEncrypt()", i);
        REALM_ASSERT_RELEASE_EX(cbData == encryption_page_size && "BCryptEncrypt()", cbData);
    }
    else if (mode == mode_Decrypt) {
        i = BCryptDecrypt(m_aes_key_handle, (PUCHAR)src, encryption_page_size, nullptr, (PUCHAR)iv, sizeof(iv),
                          (PUCHAR)dst, encryption_page_size, &cbData, 0);
        REALM_ASSERT_RELEASE_EX(i == 0 && "BCryptDecrypt()", i);
        REALM_ASSERT_RELEASE_EX(cbData == encryption_page_size && "BCryptDecrypt()", cbData);
    }
    else {
        REALM_UNREACHABLE();
    }

#else
    if (!EVP_CipherInit_ex(m_ctx, EVP_aes_256_cbc(), NULL, m_key.data(), iv, mode))
        handle_error();

    int len;
    // Use zero padding - we always write a whole page
    EVP_CIPHER_CTX_set_padding(m_ctx, 0);

    if (!EVP_CipherUpdate(m_ctx, reinterpret_cast<uint8_t*>(dst), &len, reinterpret_cast<const uint8_t*>(src),
                          encryption_page_size))
        handle_error();

    // Finalize the encryption. Should not output further data.
    if (!EVP_CipherFinal_ex(m_ctx, reinterpret_cast<uint8_t*>(dst) + len, &len))
        handle_error();
#endif
}

EncryptedFile::EncryptedFile(const char* key, FileDesc fd)
    : fd(fd)
    , cryptor(key)
{
}

std::unique_ptr<EncryptedFileMapping> EncryptedFile::add_mapping(SizeType file_offset, void* addr, size_t size,
                                                                 File::AccessMode access)
{
    auto mapping = std::make_unique<EncryptedFileMapping>(*this, file_offset, addr, size, access);
    CheckedLockGuard lock(mutex);
    mappings.push_back(mapping.get());
    return mapping;
}

EncryptedFileMapping::EncryptedFileMapping(EncryptedFile& file, SizeType file_offset, void* addr, size_t size,
                                           File::AccessMode access, util::WriteObserver* observer,
                                           util::WriteMarker* marker)
    : m_file(file)
    , m_access(access)
    , m_observer(observer)
    , m_marker(marker)
#ifdef REALM_DEBUG
    , m_validate_buffer(new char[encryption_page_size])
#endif
{
    set(addr, size, file_offset); // throws
}

EncryptedFileMapping::~EncryptedFileMapping()
{
    CheckedLockGuard lock(m_file.mutex);
    for (auto& e : m_page_state) {
        REALM_ASSERT(is_not(e, Writable));
    }
    if (m_access == File::access_ReadWrite) {
        do_flush();
    }

    auto it = std::find(m_file.mappings.begin(), m_file.mappings.end(), this);
    REALM_ASSERT(it != m_file.mappings.end());
    if (it != m_file.mappings.end()) {
        m_file.mappings.erase(it);
    }
}

// offset within page, not within file
uint16_t EncryptedFileMapping::get_offset_of_address(const void* addr) const noexcept
{
    return reinterpret_cast<uintptr_t>(addr) & (encryption_page_size - 1);
}

size_t EncryptedFileMapping::get_local_index_of_address(const void* addr, size_t offset) const noexcept
{
    REALM_ASSERT_EX(addr >= m_addr, addr, m_addr);
    return (reinterpret_cast<uintptr_t>(addr) - reinterpret_cast<uintptr_t>(m_addr) + offset) / encryption_page_size;
}

bool EncryptedFileMapping::contains_page(size_t block_in_file) const noexcept
{
    return block_in_file - m_first_page < m_page_state.size();
}

char* EncryptedFileMapping::page_addr(size_t local_ndx) const noexcept
{
    REALM_ASSERT_DEBUG(local_ndx < m_page_state.size());
    return static_cast<char*>(m_addr) + (local_ndx * encryption_page_size);
}

SizeType EncryptedFileMapping::page_pos(size_t local_ndx) const noexcept
{
    return SizeType(local_ndx + m_first_page) * encryption_page_size;
}

// If we have multiple mappings for the same part of the file, one of them may
// already contain the page we're about to read and if so we can skip reading
// it and instead just memcpy it.
bool EncryptedFileMapping::copy_up_to_date_page(size_t local_ndx) noexcept
{
    REALM_ASSERT_EX(local_ndx < m_page_state.size(), local_ndx, m_page_state.size());
    // Precondition: this method must never be called for a page which
    // is already up to date.
    REALM_ASSERT(is_not(m_page_state[local_ndx], UpToDate));
    size_t ndx_in_file = local_ndx + m_first_page;
    for (auto& m : m_file.mappings) {
        m->assert_locked();
        if (m == this || !m->contains_page(ndx_in_file))
            continue;

        size_t other_mapping_ndx = ndx_in_file - m->m_first_page;
        auto other_state = m->m_page_state[other_mapping_ndx];
        if (is(other_state, Writable) || is_not(other_state, UpToDate))
            continue;

        memcpy_if_changed(page_addr(local_ndx), m->page_addr(other_mapping_ndx), encryption_page_size);
        set(m_page_state[local_ndx], UpToDate);
        clear(m_page_state[local_ndx], StaleIV);
        return true;
    }
    return false;
}

// Whenever we advance our reader view of the file we mark all previously
// up-to-date pages as being possibly stale. On the next access of the page we
// then check if the IV for that page has changed to determine if the page has
// actually changed or if we can just mark it as being up-to-date again.
bool EncryptedFileMapping::check_possibly_stale_page(size_t local_ndx) noexcept
{
    if (is_not(m_page_state[local_ndx], StaleIV))
        return false;
    size_t ndx_in_file = local_ndx + m_first_page;
    bool did_change = m_file.cryptor.refresh_iv(m_file.fd, ndx_in_file);
    // Update the page state in all mappings and not just the current one because
    // refresh_iv() only returns true once per page per write. Deferring this
    // until copy_up_to_date_page() almost works, but this mapping could be
    // removed before the other mapping copies the page.
    for (auto& m : m_file.mappings) {
        m->assert_locked();
        if (!m->contains_page(ndx_in_file))
            continue;
        auto& state = m->m_page_state[ndx_in_file - m->m_first_page];
        if (is(state, StaleIV)) {
            REALM_ASSERT(is_not(state, UpToDate));
            clear(state, StaleIV);
            if (!did_change)
                set(state, UpToDate);
        }
    }
    return !did_change;
}

REALM_NORETURN
REALM_COLD
void EncryptedFileMapping::throw_decryption_error(size_t local_ndx, std::string_view msg)
{
    size_t fs = to_size_t(File::get_size_static(m_file.fd));
    throw DecryptionFailed(util::format("page %1 in file of size %2 %3", local_ndx + m_first_page, fs, msg));
}

void EncryptedFileMapping::refresh_page(size_t local_ndx, bool to_modify)
{
    REALM_ASSERT_EX(local_ndx < m_page_state.size(), local_ndx, m_page_state.size());
    REALM_ASSERT(is_not(m_page_state[local_ndx], Dirty));
    REALM_ASSERT(is_not(m_page_state[local_ndx], Writable));
    if (copy_up_to_date_page(local_ndx) || check_possibly_stale_page(local_ndx)) {
        return;
    }

    char* addr = page_addr(local_ndx);
    switch (m_file.cryptor.read(m_file.fd, page_pos(local_ndx), addr, m_observer)) {
        case AESCryptor::ReadResult::Eof:
            if (!to_modify)
                throw_decryption_error(local_ndx, "is out of bounds");
            break;
        case AESCryptor::ReadResult::Uninitialized:
            if (!to_modify)
                throw_decryption_error(local_ndx, "has never been written to");
            break;
        case AESCryptor::ReadResult::InterruptedFirstWrite:
            if (!to_modify)
                throw_decryption_error(local_ndx, "has never been successfully written to, but a write was begun");
            break;
        case AESCryptor::ReadResult::StaleHmac:
            break;
        case AESCryptor::ReadResult::Failed:
            throw_decryption_error(
                local_ndx, "failed the HMAC check. Either the encryption key is incorrect or data is corrupted");
        case AESCryptor::ReadResult::Success:
            break;
    }
    set(m_page_state[local_ndx], UpToDate);
}

void EncryptedFile::mark_data_as_possibly_stale()
{

    util::CheckedLockGuard lock(mutex);
    cryptor.invalidate_ivs();
    for (auto& m : mappings) {
        m->assert_locked();
        m->mark_pages_for_iv_check();
    }
}

void EncryptedFileMapping::mark_pages_for_iv_check()
{
    for (auto& state : m_page_state) {
        if (is(state, UpToDate) && is_not(state, Dirty | Writable)) {
            REALM_ASSERT(is_not(state, StaleIV));
            clear(state, UpToDate);
            set(state, StaleIV);
        }
    }
}

void EncryptedFileMapping::write_and_update_all(size_t local_ndx, uint16_t offset, uint16_t size) noexcept
{
    REALM_ASSERT(is(m_page_state[local_ndx], Writable));
    REALM_ASSERT(is(m_page_state[local_ndx], UpToDate));
    REALM_ASSERT(is_not(m_page_state[local_ndx], StaleIV));
    REALM_ASSERT(offset + size <= encryption_page_size);
    // Go through all other mappings of this file and copy changes into those mappings
    size_t ndx_in_file = local_ndx + m_first_page;
    for (auto& m : m_file.mappings) {
        m->assert_locked();
        if (m == this || !m->contains_page(ndx_in_file))
            continue;

        size_t other_local_ndx = ndx_in_file - m->m_first_page;
        auto& state = m->m_page_state[other_local_ndx];
        if (is(state, UpToDate)) {
            memcpy_if_changed(m->page_addr(other_local_ndx) + offset, page_addr(local_ndx) + offset, size);
        }
        // If the target page is possibly stale then we need to copy the entire
        // page and not just the bytes we just touched as other parts of the
        // page may be out of date
        else if (is(state, StaleIV)) {
            memcpy_if_changed(m->page_addr(other_local_ndx), page_addr(local_ndx), encryption_page_size);
            set(state, UpToDate);
            clear(state, StaleIV);
        }
    }
    set(m_page_state[local_ndx], Dirty);
    clear(m_page_state[local_ndx], Writable);
}

void EncryptedFileMapping::validate_page(size_t local_ndx) noexcept
{
#ifdef REALM_DEBUG
    REALM_ASSERT(local_ndx < m_page_state.size());
    if (is_not(m_page_state[local_ndx], UpToDate))
        return;

    switch (m_file.cryptor.read(m_file.fd, page_pos(local_ndx), m_validate_buffer.get(), m_observer)) {
        case AESCryptor::ReadResult::Eof:
        case AESCryptor::ReadResult::Uninitialized:
        case AESCryptor::ReadResult::InterruptedFirstWrite:
        case AESCryptor::ReadResult::StaleHmac:
            return;
        case AESCryptor::ReadResult::Failed:
            abort();
        case AESCryptor::ReadResult::Success:
            break;
    }

    const size_t ndx_in_file = local_ndx + m_first_page;
    for (auto& m : m_file.mappings) {
        m->assert_locked();
        size_t other_local_ndx = ndx_in_file - m->m_first_page;
        if (m != this && m->contains_page(ndx_in_file) && is(m->m_page_state[other_local_ndx], Dirty)) {
            memcpy(m_validate_buffer.get(), m->page_addr(other_local_ndx), encryption_page_size);
            break;
        }
    }

    if (memcmp(m_validate_buffer.get(), page_addr(local_ndx), encryption_page_size) != 0) {
        util::format(std::cerr, "mismatch %1: fd(%2) page(%3/%4) %5 %6\n", this, m_file.fd, local_ndx,
                     m_page_state.size(), m_validate_buffer.get(), page_addr(local_ndx));
        REALM_TERMINATE("");
    }
#else
    static_cast<void>(local_ndx);
#endif
}

void EncryptedFileMapping::validate() noexcept
{
#ifdef REALM_DEBUG
    for (size_t i = 0; i < m_page_state.size(); ++i)
        validate_page(i);
#endif
}

void EncryptedFileMapping::do_flush(bool skip_validate) noexcept
{
    for (size_t i = 0; i < m_page_state.size(); ++i) {
        if (is_not(m_page_state[i], Dirty)) {
            if (!skip_validate) {
                validate_page(i);
            }
            continue;
        }
        m_file.cryptor.write(m_file.fd, page_pos(i), page_addr(i), m_marker);
        clear(m_page_state[i], Dirty);
    }

    // some of the tests call flush() on very small writes which results in
    // validating on every flush being unreasonably slow
    if (!skip_validate) {
        validate();
    }
}

void EncryptedFileMapping::flush(bool skip_validate) noexcept
{
    util::CheckedLockGuard lock(m_file.mutex);
    do_flush(skip_validate);
}

void EncryptedFileMapping::sync() noexcept
{
    util::CheckedLockGuard lock(m_file.mutex);
    do_sync();
}

#ifdef _MSC_VER
#pragma warning(disable : 4297) // throw in noexcept
#endif
void EncryptedFileMapping::do_sync() noexcept
{
    do_flush();

#ifdef _WIN32
    if (FlushFileBuffers(m_file.fd))
        return;
    throw std::system_error(GetLastError(), std::system_category(), "FlushFileBuffers() failed");
#else
    fsync(m_file.fd);
#endif
}
#ifdef _MSC_VER
#pragma warning(default : 4297)
#endif

void EncryptedFileMapping::write_barrier(const void* addr, size_t size) noexcept
{
    CheckedLockGuard lock(m_file.mutex);
    REALM_ASSERT(size > 0);
    REALM_ASSERT(m_access == File::access_ReadWrite);

    size_t local_ndx = get_local_index_of_address(addr);
    auto offset_in_page = uint16_t(static_cast<const char*>(addr) - page_addr(local_ndx));
    size += offset_in_page;

    // Propagate changes to all other decrypted pages mapping the same memory
    while (size > 0) {
        REALM_ASSERT(local_ndx < m_page_state.size());
        REALM_ASSERT(is(m_page_state[local_ndx], PageState::Writable));
        auto bytes_in_page = uint16_t(std::min<size_t>(encryption_page_size, size) - offset_in_page);
        write_and_update_all(local_ndx, offset_in_page, bytes_in_page);
        size -= offset_in_page + bytes_in_page;
        offset_in_page = 0;
        ++local_ndx;
    }
}

void EncryptedFileMapping::read_barrier(const void* addr, size_t size, bool to_modify)
{
    CheckedLockGuard lock(m_file.mutex);
    REALM_ASSERT(size > 0);
    size_t begin = get_local_index_of_address(addr);
    size_t end = get_local_index_of_address(addr, size - 1);
    for (size_t local_ndx = begin; local_ndx <= end; ++local_ndx) {
        PageState& ps = m_page_state[local_ndx];
        if (is_not(ps, UpToDate))
            refresh_page(local_ndx, to_modify);
        if (to_modify)
            set(ps, Writable);
    }
}

void EncryptedFileMapping::extend_to(SizeType offset, size_t new_size)
{
    CheckedLockGuard lock(m_file.mutex);
    REALM_ASSERT_EX(new_size % encryption_page_size == 0, new_size, encryption_page_size);
    m_page_state.resize(page_count(new_size), PageState::Clean);
    m_file.cryptor.set_data_size(offset + SizeType(new_size));
}

void EncryptedFileMapping::set(void* new_addr, size_t new_size, SizeType new_file_offset)
{
    CheckedLockGuard lock(m_file.mutex);
    REALM_ASSERT(new_file_offset % encryption_page_size == 0);
    REALM_ASSERT(new_size % encryption_page_size == 0);

    // This seems dangerous - correct operation in a setting with multiple (partial)
    // mappings of the same file would rely on ordering of individual mapping requests.
    // Currently we only ever extend the file - but when we implement continuous defrag,
    // this design should be revisited.
    m_file.cryptor.set_data_size(new_file_offset + SizeType(new_size));

    do_flush();
    m_addr = new_addr;

    // set_data_size() would have thrown if this cast would overflow
    m_first_page = size_t(new_file_offset / encryption_page_size);
    m_page_state.clear();
    m_page_state.resize(new_size / encryption_page_size, PageState::Clean);
}

SizeType encrypted_size_to_data_size(SizeType size) noexcept
{
    return size == 0 ? 0 : file_pos_to_data_pos(size);
}

SizeType data_size_to_encrypted_size(SizeType size) noexcept
{
    SizeType r = size % encryption_page_size;
    size += r ? encryption_page_size - r : 0;
    return data_pos_to_file_pos(size);
}
} // namespace realm::util
#else

namespace realm::util {
File::SizeType encrypted_size_to_data_size(File::SizeType size) noexcept
{
    return size;
}

File::SizeType data_size_to_encrypted_size(File::SizeType size) noexcept
{
    return size;
}
} // namespace realm::util
#endif // REALM_ENABLE_ENCRYPTION

namespace realm::util {
std::string DecryptionFailed::get_message_with_bt(std::string_view msg)
{
    auto bt = Backtrace::capture();
    std::stringstream ss;
    bt.print(ss);
    return util::format("Decryption failed: %1\n%2\n", msg, ss.str());
}
} // namespace realm::util

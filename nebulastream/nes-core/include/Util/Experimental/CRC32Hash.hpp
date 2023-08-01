/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_
#define NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_
#include <Util/Experimental/Hash.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Experimental {

#if !defined(__SSE4_2__)
class CRC32Hash : public Hash<CRC32Hash> {
  public:
    inline uint64_t hashKey(uint64_t, hash_t) const { NES_NOT_IMPLEMENTED(); }
    inline uint64_t hashKey(uint64_t k) const { return hashKey(k, 0); }

    inline uint64_t hashKey(const void*, int, uint64_t) const { NES_NOT_IMPLEMENTED(); }
};

#else

#include <x86intrin.h>

/**
 * @brief CRC32Hash implementation origins from Kersten et.al. https://github.com/TimoKersten/db-engine-paradigms.
 * @note Currently the implementation only supports __x86_64__ as it relies on _mm_crc32 intrinsics.
 */
class CRC32Hash : public Hash<CRC32Hash> {
  public:
    inline auto hashKey(uint64_t k, hash_t seed) const {
        // inline hash_t hashKey(uint64_t k, uint64_t seed) const {
        uint64_t result1 = _mm_crc32_u64(seed, k);
        uint64_t result2 = _mm_crc32_u64(0x04c11db7, k);
        return ((result2 << 32) | result1) * 0x2545F4914F6CDD1Dull;
    }
    inline uint64_t hashKey(uint64_t k) const { return hashKey(k, 0); }

    inline uint64_t hashKey(const void* key, int len, uint64_t seed) const {
        auto data = reinterpret_cast<const uint8_t*>(key);
        uint64_t s = seed;
        while (len >= 8) {
            s = hashKey(*reinterpret_cast<const uint64_t*>(data), s);
            data += 8;
            len -= 8;
        }
        if (len >= 4) {
            s = hashKey((uint32_t) * reinterpret_cast<const uint32_t*>(data), s);
            data += 4;
            len -= 4;
        }
        switch (len) {
            case 3: s ^= ((uint64_t) data[2]) << 16;
            case 2: s ^= ((uint64_t) data[1]) << 8;
            case 1: s ^= data[0];
        }
        return s;
    }
};
#endif
}// namespace NES::Experimental

#endif// NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_CRC32HASH_HPP_

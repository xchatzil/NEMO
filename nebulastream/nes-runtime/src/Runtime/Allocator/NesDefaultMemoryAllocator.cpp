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

#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>

namespace NES::Runtime {

void* NesDefaultMemoryAllocator::do_allocate(size_t bytes, size_t alignment) {
    void* tmp = nullptr;
    NES_ASSERT(posix_memalign(&tmp, alignment, bytes) == 0, "memory allocation failed with alignment");
    return tmp;
}

void NesDefaultMemoryAllocator::do_deallocate(void* p, size_t, size_t) { std::free(p); }

}// namespace NES::Runtime
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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

Scan::Scan(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider, std::vector<Record::RecordFieldIdentifier> projections)
    : memoryProvider(std::move(memoryProvider)), projections(projections) {}

void Scan::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const {
    // call open on all child operators
    child->open(ctx, recordBuffer);
    // iterate over records in buffer
    auto numberOfRecords = recordBuffer.getNumRecords();
    auto bufferAddress = recordBuffer.getBuffer();
    for (Value<UInt64> i = (uint64_t) 0; i < numberOfRecords; i = i + (uint64_t) 1) {
        auto record = memoryProvider->read(projections, bufferAddress, i);
        child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators
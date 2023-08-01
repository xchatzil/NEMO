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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_SCAN_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_SCAN_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Operator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This basic scan operator extracts records from a base tuple buffer according to a memory layout.
 * Furthermore, it supports projection pushdown to eliminate unneeded reads.
 */
class Scan : public Operator {
  public:
    /**
     * @brief Constructor for the scan operator that receives a memory layout and a projection vector.
     * @param memoryLayout memory layout that describes the tuple buffer.
     * @param projections projection vector
     */
    Scan(std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider,
         std::vector<Nautilus::Record::RecordFieldIdentifier> projections = {});

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  private:
    const std::unique_ptr<MemoryProvider::MemoryProvider> memoryProvider;
    const std::vector<Nautilus::Record::RecordFieldIdentifier> projections;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_SCAN_HPP_

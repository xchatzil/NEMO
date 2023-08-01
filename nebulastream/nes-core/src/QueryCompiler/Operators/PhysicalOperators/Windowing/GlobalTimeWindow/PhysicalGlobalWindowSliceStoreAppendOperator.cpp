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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <memory>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalGlobalWindowSliceStoreAppendOperator::PhysicalGlobalWindowSliceStoreAppendOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalWindowGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema), AbstractEmitOperator(),
      windowHandler(keyedEventTimeWindowHandler) {}

std::shared_ptr<PhysicalGlobalWindowSliceStoreAppendOperator> PhysicalGlobalWindowSliceStoreAppendOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalWindowGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalGlobalWindowSliceStoreAppendOperator>(Util::getNextOperatorId(),
                                                                          inputSchema,
                                                                          outputSchema,
                                                                          keyedEventTimeWindowHandler);
}

std::string PhysicalGlobalWindowSliceStoreAppendOperator::toString() const {
    return "PhysicalGlobalWindowSliceStoreAppendOperator";
}

OperatorNodePtr PhysicalGlobalWindowSliceStoreAppendOperator::copy() { return create(inputSchema, outputSchema, windowHandler); }

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
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

#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowOperator.hpp>
#include <Util/UtilityFunctions.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

PhysicalGlobalThreadLocalPreAggregationOperator::PhysicalGlobalThreadLocalPreAggregationOperator(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandlerPtr keyedEventTimeWindowHandler)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, outputSchema),
      keyedEventTimeWindowHandler(keyedEventTimeWindowHandler) {}
std::string PhysicalGlobalThreadLocalPreAggregationOperator::toString() const {
    return "PhysicalGlobalThreadLocalPreAggregationOperator";
}

std::shared_ptr<PhysicalOperator> PhysicalGlobalThreadLocalPreAggregationOperator::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::Experimental::GlobalThreadLocalPreAggregationOperatorHandlerPtr keyedEventTimeWindowHandler) {
    return std::make_shared<PhysicalGlobalThreadLocalPreAggregationOperator>(Util::getNextOperatorId(),
                                                                             inputSchema,
                                                                             outputSchema,
                                                                             keyedEventTimeWindowHandler);
}

OperatorNodePtr PhysicalGlobalThreadLocalPreAggregationOperator::copy() {
    return create(inputSchema, outputSchema, keyedEventTimeWindowHandler);
}
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
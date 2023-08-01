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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <utility>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalScanOperator::PhysicalScanOperator(OperatorId id, const SchemaPtr& outputSchema)
    : OperatorNode(id), PhysicalUnaryOperator(id, outputSchema, outputSchema) {}

PhysicalOperatorPtr PhysicalScanOperator::create(SchemaPtr outputSchema) {
    return create(Util::getNextOperatorId(), std::move(outputSchema));
}
PhysicalOperatorPtr PhysicalScanOperator::create(OperatorId id, const SchemaPtr& outputSchema) {
    return std::make_shared<PhysicalScanOperator>(id, outputSchema);
}

std::string PhysicalScanOperator::toString() const { return "PhysicalScanOperator"; }

OperatorNodePtr PhysicalScanOperator::copy() { return create(id, outputSchema); }

}// namespace NES::QueryCompilation::PhysicalOperators
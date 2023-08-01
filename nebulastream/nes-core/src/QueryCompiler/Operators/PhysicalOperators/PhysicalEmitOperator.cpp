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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <utility>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalEmitOperator::PhysicalEmitOperator(OperatorId id, const SchemaPtr& inputSchema)
    : OperatorNode(id), PhysicalUnaryOperator(id, inputSchema, inputSchema) {}

PhysicalOperatorPtr PhysicalEmitOperator::create(SchemaPtr inputSchema) {
    return create(Util::getNextOperatorId(), std::move(inputSchema));
}
PhysicalOperatorPtr PhysicalEmitOperator::create(OperatorId id, const SchemaPtr& inputSchema) {
    return std::make_shared<PhysicalEmitOperator>(id, inputSchema);
}

std::string PhysicalEmitOperator::toString() const { return "PhysicalEmitOperator"; }

OperatorNodePtr PhysicalEmitOperator::copy() { return create(id, inputSchema); }

}// namespace NES::QueryCompilation::PhysicalOperators
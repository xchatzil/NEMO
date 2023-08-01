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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <utility>
namespace NES::QueryCompilation::PhysicalOperators {

PhysicalProjectOperator::PhysicalProjectOperator(OperatorId id,
                                                 SchemaPtr inputSchema,
                                                 SchemaPtr outputSchema,
                                                 std::vector<ExpressionNodePtr> expressions)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      expressions(std::move(expressions)) {}

PhysicalOperatorPtr PhysicalProjectOperator::create(OperatorId id,
                                                    const SchemaPtr& inputSchema,
                                                    const SchemaPtr& outputSchema,
                                                    const std::vector<ExpressionNodePtr>& expressions) {
    return std::make_shared<PhysicalProjectOperator>(id, inputSchema, outputSchema, expressions);
}

PhysicalOperatorPtr
PhysicalProjectOperator::create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<ExpressionNodePtr> expressions) {
    return create(Util::getNextOperatorId(), std::move(inputSchema), std::move(outputSchema), std::move(expressions));
}

std::vector<ExpressionNodePtr> PhysicalProjectOperator::getExpressions() { return expressions; }

std::string PhysicalProjectOperator::toString() const { return "PhysicalProjectOperator"; }

OperatorNodePtr PhysicalProjectOperator::copy() { return create(id, inputSchema, outputSchema, getExpressions()); }

}// namespace NES::QueryCompilation::PhysicalOperators
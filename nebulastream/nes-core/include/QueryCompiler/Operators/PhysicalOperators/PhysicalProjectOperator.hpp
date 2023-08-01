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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPROJECTOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPROJECTOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {

/**
 * @brief Physical projection operator.
 */
class PhysicalProjectOperator : public PhysicalUnaryOperator {
  public:
    PhysicalProjectOperator(OperatorId id,
                            SchemaPtr inputSchema,
                            SchemaPtr outputSchema,
                            std::vector<ExpressionNodePtr> expressions);
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      const std::vector<ExpressionNodePtr>& expressions);
    static PhysicalOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<ExpressionNodePtr> expressions);
    /**
     * @brief returns the list of fields that remain in the output schema.
     * @return  std::vector<ExpressionNodePtr>
     */
    std::vector<ExpressionNodePtr> getExpressions();
    std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    std::vector<ExpressionNodePtr> expressions;
};

}// namespace NES::QueryCompilation::PhysicalOperators

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPROJECTOPERATOR_HPP_

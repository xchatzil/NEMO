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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates the projection operator, to select or rename certain attributes of an input tuple.
 */
class GeneratableProjectionOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable projection operator, which projects the set of input fields.
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param expressions the projections
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<ExpressionNodePtr> expressions);

    /**
     * @brief Creates a new generatable projection operator, which projects the set of input fields.
     * @param id operator id
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param expressions the projections
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, std::vector<ExpressionNodePtr> expressions);
    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

    ~GeneratableProjectionOperator() noexcept override = default;

  private:
    GeneratableProjectionOperator(OperatorId id,
                                  SchemaPtr inputSchema,
                                  SchemaPtr outputSchema,
                                  std::vector<ExpressionNodePtr> expressions);
    std::vector<ExpressionNodePtr> expressions;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEPROJECTIONOPERATOR_HPP_

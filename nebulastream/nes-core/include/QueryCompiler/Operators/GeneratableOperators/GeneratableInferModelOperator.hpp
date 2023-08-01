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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFERMODELOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFERMODELOPERATOR_HPP_

#include <Nodes/Expressions/ExpressionNode.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>
#include <Windowing/JoinForwardRefs.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Generates a conditional inferModel operator, which infers an ML model
 */
class GeneratableInferModelOperator : public GeneratableOperator {
  public:
    static GeneratableOperatorPtr create(SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         std::string model,
                                         std::vector<ExpressionItemPtr> inputFields,
                                         std::vector<ExpressionItemPtr> outputFields,
                                         InferModel::InferModelOperatorHandlerPtr operatorHandler);
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         std::string model,
                                         std::vector<ExpressionItemPtr> inputFields,
                                         std::vector<ExpressionItemPtr> outputFields,
                                         InferModel::InferModelOperatorHandlerPtr operatorHandler);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableInferModelOperator(OperatorId id,
                                  SchemaPtr inputSchema,
                                  SchemaPtr outputSchema,
                                  std::string model,
                                  std::vector<ExpressionItemPtr> inputFields,
                                  std::vector<ExpressionItemPtr> outputFields,
                                  InferModel::InferModelOperatorHandlerPtr operatorHandler);
    const std::string model;
    const std::vector<ExpressionItemPtr> inputFields;
    const std::vector<ExpressionItemPtr> outputFields;
    InferModel::InferModelOperatorHandlerPtr operatorHandler;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEINFERMODELOPERATOR_HPP_

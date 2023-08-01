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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinOperator.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Defines the code generation for the build side of a join operator.
 */
class GeneratableJoinBuildOperator : public GeneratableJoinOperator {
  public:
    /**
     * @brief Creates a new generatable join build operator.
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param operatorHandler the join operator handler
     * @param buildSide indicator if this is the left or right build side.
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler, JoinBuildSide buildSide);

    /**
     * @brief Creates a new generatable join build operator.
     * @param id operator id
     * @param inputSchema the input schema for the operator.
     * @param outputSchema the output schema for the operator.
     * @param operatorHandler the join operator handler
     * @param buildSide indicator if this is the left or right build side.
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Join::JoinOperatorHandlerPtr operatorHandler,
                                         JoinBuildSide buildSide);

    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
     * @brief Code generation function for the open call of an operator.
     * The open function is called once per operator to initialize local state by a single thread.
     * If called on the left join side, it initializes the Join sink. On the right side it does nothing.
     * @param codegen reference to the code generator.
     * @param context reference to the current pipeline context.
     */
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  protected:
    GeneratableJoinBuildOperator(OperatorId id,
                                 SchemaPtr inputSchema,
                                 SchemaPtr outputSchema,
                                 Join::JoinOperatorHandlerPtr operatorHandler,
                                 JoinBuildSide buildSide);
    JoinBuildSide buildSide;
};
}// namespace NES::QueryCompilation::GeneratableOperators
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_JOINING_GENERATABLEJOINBUILDOPERATOR_HPP_

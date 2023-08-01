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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>
#include <QueryCompiler/Phases/OutputBufferAllocationStrategies.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Generates the emit operator, which outputs a tuple buffer to the next pipeline.
 */
class GeneratableBufferEmit : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable emit buffer, which emits record according to a specific output schema.
     * @param outputSchema of the result records
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr outputSchema);

    /**
    * @brief Creates a new generatable emit buffer, which emits record according to a specific output schema.
    * @param id operator id
    * @param outputSchema of the result records
    * @return GeneratableOperatorPtr
    */
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr outputSchema);

    /**
     * @brief get the output buffer allocation strategy
     * @return OutputBufferAllocationStrategy
     */
    OutputBufferAllocationStrategy getOutputBufferAllocationStrategy() const;

    /**
     * @brief sets the output buffer allocation strategy
     * @param strategy
     */
    void setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy strategy);

    /**
     * @brief get the output buffer assignment strategy
     * @return OutputBufferAssignmentStrategy
     */
    OutputBufferAssignmentStrategy getOutputBufferAssignmentStrategy() const;

    /**
     * @brief sets the output buffer assignment strategy
     * @param strategy
     */
    void setOutputBufferAssignmentStrategy(OutputBufferAssignmentStrategy strategy);

    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
    ~GeneratableBufferEmit() noexcept override = default;

  private:
    GeneratableBufferEmit(OperatorId id, const SchemaPtr& outputSchema);
    OutputBufferAllocationStrategy bufferAllocationStrategy;
    OutputBufferAssignmentStrategy bufferAssignmentStrategy = FIELD_COPY;
};
}// namespace NES::QueryCompilation::GeneratableOperators
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEBUFFEREMIT_HPP_

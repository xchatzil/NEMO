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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEMEDIANAGGREGATION_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEMEDIANAGGREGATION_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

class GeneratableMedianAggregation : public GeneratableWindowAggregation {
  public:
    explicit GeneratableMedianAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);
    ~GeneratableMedianAggregation() noexcept override = default;

    /**
     * @brief Factory Method to create a new GeneratableWindowAggregation
     * @param aggregationDescriptor Window aggregation descriptor
     * @return GeneratableWindowAggregationPtr
     */
    static GeneratableWindowAggregationPtr create(const Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);

    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialRef partial value ref
     * @param recordHandler record handler
     */
    void compileLiftCombine(CompoundStatementPtr currentCode,
                            BinaryOperatorStatement partialRef,
                            RecordHandlerPtr recordHandler) override;
    /**
     * @brief Generate code for combine function to combine multiple pre-aggregates to each other
     * @param currentCode
     * @param partialValueRef1
     * @param partialValueRef2
     */
    void
    compileCombine(CompoundStatementPtr currentCode, VarRefStatement partialValueRef1, VarRefStatement partialValueRef2) override;

    /**
     * @brief Generate code to initialize a window aggregate, based on the initial value.
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    void compileLift(CompoundStatementPtr currentCode,
                     BinaryOperatorStatement partialValueRef,
                     RecordHandlerPtr recordHandler) override;
    VariableDeclarationPtr getPartialAggregate() override;
};

}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_AGGREGATIONS_GENERATABLEMEDIANAGGREGATION_HPP_

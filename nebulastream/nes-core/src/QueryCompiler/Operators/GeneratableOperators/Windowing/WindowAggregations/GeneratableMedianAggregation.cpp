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
#include "QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp"
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMedianAggregation.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableMedianAggregation::GeneratableMedianAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor)
    : GeneratableWindowAggregation(std::move(aggregationDescriptor)) {}

GeneratableWindowAggregationPtr
GeneratableMedianAggregation::create(const Windowing::WindowAggregationDescriptorPtr aggregationDescriptor) {
    return std::make_shared<GeneratableMedianAggregation>(aggregationDescriptor);
}
void GeneratableMedianAggregation::compileLift(CompoundStatementPtr currentCode,
                                               BinaryOperatorStatement partialRef,
                                               RecordHandlerPtr) {
    auto newCall = FunctionCallStatement("new NES::Windowing::PartialMedian");
    newCall.addParameter(*partialRef);
    currentCode->addStatement(newCall.copy());
}

void GeneratableMedianAggregation::compileLiftCombine(CompoundStatementPtr currentCode,
                                                      BinaryOperatorStatement partialRef,
                                                      RecordHandlerPtr recordHandler) {
    // generates the following code:
    // partialAggregates[current_slice_index].push_back(inputTuples[recordIndex].car$value);
    auto fieldReference =
        recordHandler->getAttribute(aggregationDescriptor->on()->as<FieldAccessExpressionNode>()->getFieldName());

    auto pushBackFunctionCall = FunctionCallStatement("push_back");
    pushBackFunctionCall.addParameter(*fieldReference);

    auto updatedPartial = partialRef.accessRef(pushBackFunctionCall);

    currentCode->addStatement(updatedPartial.copy());
}

VariableDeclarationPtr GeneratableMedianAggregation::getPartialAggregate() {
    auto tf = GeneratableTypesFactory();
    return std::make_shared<VariableDeclaration>(
        VariableDeclaration::create(tf.createAnonymusDataType("NES::Windowing::PartialMedian"), "median_value"));
}

void GeneratableMedianAggregation::compileCombine(CompoundStatementPtr currentCode,
                                                  VarRefStatement partialValueRef1,
                                                  VarRefStatement partialValueRef2) {
    auto updatedPartial = partialValueRef1.accessPtr(VarRef(getPartialAggregate()))
                              .assign(partialValueRef1.accessPtr(VarRef(getPartialAggregate()))
                                      + partialValueRef2.accessPtr(VarRef(getPartialAggregate())));
    currentCode->addStatement(updatedPartial.copy());
}
}// namespace NES::QueryCompilation::GeneratableOperators
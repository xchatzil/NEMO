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

#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {
GeneratableOperatorPtr GeneratableGlobalTumblingWindowSink::create(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return std::make_shared<GeneratableGlobalTumblingWindowSink>(
        GeneratableGlobalTumblingWindowSink(id,
                                            std::move(inputSchema),
                                            std::move(outputSchema),
                                            windowDefinition,
                                            std::move(windowAggregation)));
}

GeneratableOperatorPtr GeneratableGlobalTumblingWindowSink::create(
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::LogicalWindowDefinitionPtr windowDefinition,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation) {
    return create(Util::getNextOperatorId(),
                  std::move(inputSchema),
                  std::move(outputSchema),
                  windowDefinition,
                  std::move(windowAggregation));
}

GeneratableGlobalTumblingWindowSink::GeneratableGlobalTumblingWindowSink(
    OperatorId id,
    SchemaPtr inputSchema,
    SchemaPtr outputSchema,
    Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation)
    : OperatorNode(id), GeneratableOperator(id, std::move(inputSchema), std::move(outputSchema)),
      windowAggregation(std::move(windowAggregation)), windowDefinition(windowDefinition) {}

void GeneratableGlobalTumblingWindowSink::generateOpen(CodeGeneratorPtr, PipelineContextPtr) {}

void GeneratableGlobalTumblingWindowSink::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForGlobalTumblingWindowSink(windowDefinition, windowAggregation, context, outputSchema);
}

std::string GeneratableGlobalTumblingWindowSink::toString() const { return "GeneratableKeyedSliceMergingOperator"; }

OperatorNodePtr GeneratableGlobalTumblingWindowSink::copy() {
    return create(id, inputSchema, outputSchema, windowDefinition, windowAggregation);
}

}// namespace NES::QueryCompilation::GeneratableOperators
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
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <Util/UtilityFunctions.hpp>
#include <utility>

namespace NES::QueryCompilation::GeneratableOperators {

GeneratableBufferEmit::GeneratableBufferEmit(OperatorId id, const SchemaPtr& outputSchema)
    : OperatorNode(id), GeneratableOperator(id, outputSchema, outputSchema), bufferAllocationStrategy(NO_OPTIMIZATION){};

GeneratableOperatorPtr GeneratableBufferEmit::create(OperatorId id, SchemaPtr outputSchema) {
    return std::make_shared<GeneratableBufferEmit>(GeneratableBufferEmit(id, std::move(outputSchema)));
}

GeneratableOperatorPtr GeneratableBufferEmit::create(SchemaPtr outputSchema) {
    return create(Util::getNextOperatorId(), std::move(outputSchema));
}

void GeneratableBufferEmit::generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(outputSchema, bufferAllocationStrategy, bufferAssignmentStrategy, context);
}
std::string GeneratableBufferEmit::toString() const { return "GeneratableBufferEmit"; }

OperatorNodePtr GeneratableBufferEmit::copy() { return create(id, outputSchema); }

OutputBufferAllocationStrategy GeneratableBufferEmit::getOutputBufferAllocationStrategy() const {
    return bufferAllocationStrategy;
};

void GeneratableBufferEmit::setOutputBufferAllocationStrategy(OutputBufferAllocationStrategy strategy) {
    this->bufferAllocationStrategy = strategy;
};

OutputBufferAssignmentStrategy GeneratableBufferEmit::getOutputBufferAssignmentStrategy() const {
    return this->bufferAssignmentStrategy;
};

void GeneratableBufferEmit::setOutputBufferAssignmentStrategy(OutputBufferAssignmentStrategy strategy) {
    this->bufferAssignmentStrategy = strategy;
};

}// namespace NES::QueryCompilation::GeneratableOperators
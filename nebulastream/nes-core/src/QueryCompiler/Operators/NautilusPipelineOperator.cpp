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
#include <QueryCompiler/Operators/NautilusPipelineOperator.hpp>
#include <utility>

namespace NES {
namespace QueryCompilation {

OperatorNodePtr NautilusPipelineOperator::create(std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline,
                                                 std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers) {
    return std::make_shared<NautilusPipelineOperator>(
        NautilusPipelineOperator(std::move(nautilusPipeline), std::move(operatorHandlers)));
}

NautilusPipelineOperator::NautilusPipelineOperator(std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> nautilusPipeline,
                                                   std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers)
    : OperatorNode(id), UnaryOperatorNode(id), nautilusPipeline(std::move(nautilusPipeline)),
      operatorHandlers(std::move(operatorHandlers)) {}

std::string NautilusPipelineOperator::toString() const { return "NautilusPipelineOperator"; }

OperatorNodePtr NautilusPipelineOperator::copy() { return create(nautilusPipeline, operatorHandlers); }

std::shared_ptr<Runtime::Execution::PhysicalOperatorPipeline> NautilusPipelineOperator::getNautilusPipeline() {
    return nautilusPipeline;
}

std::vector<Runtime::Execution::OperatorHandlerPtr> NautilusPipelineOperator::getOperatorHandlers() { return operatorHandlers; }

}// namespace QueryCompilation
}// namespace NES
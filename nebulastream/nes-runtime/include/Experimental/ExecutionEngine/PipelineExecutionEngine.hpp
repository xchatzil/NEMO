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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINEEXECUTIONENGINE_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINEEXECUTIONENGINE_HPP_
#include <memory>
namespace NES::ExecutionEngine::Experimental {

class ExecutablePipeline;
class PhysicalOperatorPipeline;

class PipelineExecutionEngine {

  public:
    virtual ~PipelineExecutionEngine() = default;
    /**
     * @brief Compiles a physical operator pipeline to an executable pipeline.
     * Depending on the backend this may involve code generation and compilation
     * @param physicalOperatorPipeline
     * @return ExecutablePipeline
     */
    virtual std::shared_ptr<ExecutablePipeline> compile(std::shared_ptr<PhysicalOperatorPipeline> physicalOperatorPipeline) = 0;
};

}// namespace NES::ExecutionEngine::Experimental
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_EXECUTIONENGINE_PIPELINEEXECUTIONENGINE_HPP_

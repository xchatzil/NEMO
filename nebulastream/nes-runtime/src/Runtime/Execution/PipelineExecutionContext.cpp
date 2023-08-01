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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <utility>

namespace NES::Runtime::Execution {

PipelineExecutionContext::PipelineExecutionContext(uint64_t pipelineId,
                                                   QuerySubPlanId queryId,
                                                   Runtime::BufferManagerPtr bufferProvider,
                                                   size_t numberOfWorkerThreads,
                                                   std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction,
                                                   std::function<void(TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
                                                   std::vector<OperatorHandlerPtr> operatorHandlers)
    : pipelineId(pipelineId), queryId(queryId), emitFunctionHandler(std::move(emitFunction)),
      emitToQueryManagerFunctionHandler(std::move(emitToQueryManagerFunctionHandler)),
      operatorHandlers(std::move(operatorHandlers)), bufferProvider(bufferProvider),
      numberOfWorkerThreads(numberOfWorkerThreads) {}

void PipelineExecutionContext::emitBuffer(TupleBuffer buffer, WorkerContextRef workerContext) {
    // call the function handler
    emitFunctionHandler(buffer, workerContext);
}

void PipelineExecutionContext::dispatchBuffer(TupleBuffer buffer) {
    // call the function handler
    emitToQueryManagerFunctionHandler(buffer);
}

std::vector<OperatorHandlerPtr> PipelineExecutionContext::getOperatorHandlers() { return operatorHandlers; }

std::string PipelineExecutionContext::toString() const { return "PipelineContext(queryID:" + std::to_string(queryId); }
uint64_t PipelineExecutionContext::getNumberOfWorkerThreads() const { return numberOfWorkerThreads; }
Runtime::BufferManagerPtr PipelineExecutionContext::getBufferManager() const { return bufferProvider; }

}// namespace NES::Runtime::Execution
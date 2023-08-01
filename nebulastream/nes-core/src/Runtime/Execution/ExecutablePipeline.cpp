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

#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>

using namespace std::chrono_literals;
namespace NES::Runtime::Execution {
ExecutablePipeline::ExecutablePipeline(uint64_t pipelineId,
                                       QueryId queryId,
                                       QuerySubPlanId querySubPlanId,
                                       QueryManagerPtr queryManager,
                                       PipelineExecutionContextPtr pipelineExecutionContext,
                                       ExecutablePipelineStagePtr executablePipelineStage,
                                       uint32_t numOfProducingPipelines,
                                       std::vector<SuccessorExecutablePipeline> successorPipelines,
                                       bool reconfiguration)
    : pipelineId(pipelineId), queryId(queryId), querySubPlanId(querySubPlanId), queryManager(queryManager),
      executablePipelineStage(std::move(executablePipelineStage)), pipelineContext(std::move(pipelineExecutionContext)),
      reconfiguration(reconfiguration),
      pipelineStatus(reconfiguration ? PipelineStatus::PipelineRunning : PipelineStatus::PipelineCreated),
      activeProducers(numOfProducingPipelines), successorPipelines(std::move(successorPipelines)) {
    // nop
    NES_ASSERT(this->executablePipelineStage && this->pipelineContext && numOfProducingPipelines > 0,
               "Wrong pipeline stage argument");
}

ExecutionResult ExecutablePipeline::execute(TupleBuffer& inputBuffer, WorkerContextRef workerContext) {
    NES_TRACE("Execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                << " stage=" << pipelineId);

    switch (this->pipelineStatus.load()) {
        case PipelineStatus::PipelineRunning: {
            auto res = executablePipelineStage->execute(inputBuffer, *pipelineContext.get(), workerContext);
            return res;
        }
        case PipelineStatus::PipelineStopped: {
            return ExecutionResult::Finished;
        }
        default: {
            NES_ERROR("Cannot execute Pipeline Stage with id=" << querySubPlanId << " originId=" << inputBuffer.getOriginId()
                                                               << " stage=" << pipelineId
                                                               << " as pipeline is not running anymore");
            return ExecutionResult::Error;
        }
    }
}

bool ExecutablePipeline::setup(const QueryManagerPtr&, const BufferManagerPtr&) {
    return executablePipelineStage->setup(*pipelineContext.get()) == 0;
}

bool ExecutablePipeline::start(const StateManagerPtr& stateManager) {
    auto expected = PipelineStatus::PipelineCreated;
    uint32_t localStateVariableId = 0;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineRunning)) {
        auto newReconf = ReconfigurationMessage(queryId,
                                                querySubPlanId,
                                                Initialize,
                                                inherited0::shared_from_this(),
                                                std::make_any<uint32_t>(activeProducers.load()));
        for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
            operatorHandler->start(pipelineContext, stateManager, localStateVariableId);
            localStateVariableId++;
        }
        queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, true);
        executablePipelineStage->start(*pipelineContext.get());
        return true;
    }
    return false;
}

bool ExecutablePipeline::stop(QueryTerminationType) {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineStopped)) {
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineStopped;
}

bool ExecutablePipeline::fail() {
    auto expected = PipelineStatus::PipelineRunning;
    if (pipelineStatus.compare_exchange_strong(expected, PipelineStatus::PipelineFailed)) {
        return executablePipelineStage->stop(*pipelineContext.get()) == 0;
    }
    return expected == PipelineStatus::PipelineFailed;
}

bool ExecutablePipeline::isRunning() const { return pipelineStatus.load() == PipelineStatus::PipelineRunning; }

const std::vector<SuccessorExecutablePipeline>& ExecutablePipeline::getSuccessors() const { return successorPipelines; }

void ExecutablePipeline::onEvent(Runtime::BaseEvent& event) {
    NES_DEBUG("ExecutablePipeline::onEvent(event) called. pipelineId: " << this->pipelineId);
    if (event.getEventType() == EventType::kStartSourceEvent) {
        NES_DEBUG("ExecutablePipeline: Propagate startSourceEvent further upstream to predecessors, without workerContext.");

        for (auto predecessor : this->pipelineContext->getPredecessors()) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&predecessor)) {
                NES_DEBUG(
                    "ExecutablePipeline: Found Source in predecessor. Start it with startSourceEvent, without workerContext.");
                sourcePredecessor->lock()->onEvent(event);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&predecessor)) {
                NES_DEBUG("ExecutablePipeline: Found Pipeline in Predecessors. Propagate startSourceEvent to it, without "
                          "workerContext.");
                pipelinePredecessor->lock()->onEvent(event);
            }
        }
    }
}

void ExecutablePipeline::onEvent(Runtime::BaseEvent& event, WorkerContextRef workerContext) {
    NES_DEBUG("ExecutablePipeline::onEvent(event, wrkContext) called. pipelineId: " << this->pipelineId);
    if (event.getEventType() == EventType::kStartSourceEvent) {
        NES_DEBUG("ExecutablePipeline: Propagate startSourceEvent further upstream to predecessors, with workerContext.");

        for (auto predecessor : this->pipelineContext->getPredecessors()) {
            if (const auto* sourcePredecessor = std::get_if<std::weak_ptr<NES::DataSource>>(&predecessor)) {
                NES_DEBUG("ExecutablePipeline: Found Source in predecessor. Start it with startSourceEvent, with workerContext.");
                sourcePredecessor->lock()->onEvent(event, workerContext);
            } else if (const auto* pipelinePredecessor =
                           std::get_if<std::weak_ptr<NES::Runtime::Execution::ExecutablePipeline>>(&predecessor)) {
                NES_DEBUG(
                    "ExecutablePipeline: Found Pipeline in Predecessors. Propagate startSourceEvent to it, with workerContext.");
                pipelinePredecessor->lock()->onEvent(event, workerContext);
            }
        }
    }
}

uint64_t ExecutablePipeline::getPipelineId() const { return pipelineId; }

QueryId ExecutablePipeline::getQueryId() const { return queryId; }

QuerySubPlanId ExecutablePipeline::getQuerySubPlanId() const { return querySubPlanId; }

bool ExecutablePipeline::isReconfiguration() const { return reconfiguration; }

ExecutablePipelinePtr ExecutablePipeline::create(uint64_t pipelineId,
                                                 QueryId queryId,
                                                 QuerySubPlanId querySubPlanId,
                                                 const QueryManagerPtr& queryManager,
                                                 const PipelineExecutionContextPtr& pipelineExecutionContext,
                                                 const ExecutablePipelineStagePtr& executablePipelineStage,
                                                 uint32_t numOfProducingPipelines,
                                                 const std::vector<SuccessorExecutablePipeline>& successorPipelines,
                                                 bool reconfiguration) {
    NES_ASSERT2_FMT(executablePipelineStage != nullptr,
                    "Executable pipelinestage is null for " << pipelineId
                                                            << "within the following query sub plan: " << querySubPlanId);
    NES_ASSERT2_FMT(pipelineExecutionContext != nullptr,
                    "Pipeline context is null for " << pipelineId << "within the following query sub plan: " << querySubPlanId);

    return std::make_shared<ExecutablePipeline>(pipelineId,
                                                queryId,
                                                querySubPlanId,
                                                queryManager,
                                                pipelineExecutionContext,
                                                executablePipelineStage,
                                                numOfProducingPipelines,
                                                successorPipelines,
                                                reconfiguration);
}

void ExecutablePipeline::reconfigure(ReconfigurationMessage& task, WorkerContext& context) {
    NES_DEBUG("Going to reconfigure pipeline " << pipelineId << " belonging to query id: " << querySubPlanId
                                               << " stage id: " << pipelineId);
    Reconfigurable::reconfigure(task, context);
    switch (task.getType()) {
        case Initialize: {
            NES_ASSERT2_FMT(isRunning(),
                            "Going to reconfigure a non-running pipeline "
                                << pipelineId << " belonging to query id: " << querySubPlanId << " stage id: " << pipelineId);
            auto refCnt = task.getUserData<uint32_t>();
            context.setObjectRefCnt(this, refCnt);
            break;
        }
        case FailEndOfStream:
        case HardEndOfStream:
        case SoftEndOfStream: {
            if (context.decreaseObjectRefCnt(this) == 1) {
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->reconfigure(task, context);
                }
            }
            break;
        }
        default: {
            break;
        }
    }
}

void ExecutablePipeline::postReconfigurationCallback(ReconfigurationMessage& task) {
    NES_DEBUG("Going to execute postReconfigurationCallback on pipeline belonging to subplanId: " << querySubPlanId
                                                                                                  << " stage id: " << pipelineId);
    Reconfigurable::postReconfigurationCallback(task);
    switch (task.getType()) {
        case FailEndOfStream: {
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                // mark the pipeline as failed
                fail();
                // tell the query manager about it
                queryManager->notifyPipelineCompletion(querySubPlanId,
                                                       inherited0::shared_from_this<ExecutablePipeline>(),
                                                       Runtime::QueryTerminationType::Failure);
                for (const auto& successorPipeline : successorPipelines) {
                    if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(queryId, querySubPlanId, task.getType(), *pipe);
                        queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                                  << querySubPlanId << " stage id: " << (*pipe)->getPipelineId()
                                  << " got FailEndOfStream  with nextPipeline");
                    } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(queryId, querySubPlanId, task.getType(), *sink);
                        queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next sink belonging to subplanId: "
                                  << querySubPlanId << " sink id: " << (*sink)->toString()
                                  << " got FailEndOfStream  with nextPipeline");
                    }
                }
            }
        }
        case HardEndOfStream:
        case SoftEndOfStream: {
            //we mantain a set of producers, and we will only trigger the end of stream once all producers have sent the EOS, for this we decrement the counter
            auto prevProducerCounter = activeProducers.fetch_sub(1);
            if (prevProducerCounter == 1) {//all producers sent EOS
                NES_DEBUG("Reconfiguration of pipeline belonging to subplanId: " << querySubPlanId << " stage id: " << pipelineId
                                                                                 << " reached prev=1");
                auto terminationType = task.getType() == Runtime::SoftEndOfStream ? Runtime::QueryTerminationType::Graceful
                                                                                  : Runtime::QueryTerminationType::HardStop;

                // do not change the order here
                // first, stop and drain handlers, if necessary
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->stop(terminationType, pipelineContext);
                }
                for (const auto& operatorHandler : pipelineContext->getOperatorHandlers()) {
                    operatorHandler->postReconfigurationCallback(task);
                }
                // second, stop pipeline, if not stopped yet
                stop(terminationType);
                // finally, notify query manager
                queryManager->notifyPipelineCompletion(querySubPlanId,
                                                       inherited0::shared_from_this<ExecutablePipeline>(),
                                                       terminationType);

                for (const auto& successorPipeline : successorPipelines) {
                    if (auto* pipe = std::get_if<ExecutablePipelinePtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(queryId, querySubPlanId, task.getType(), *pipe);
                        queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next pipeline belonging to subplanId: "
                                  << querySubPlanId << " stage id: " << (*pipe)->getPipelineId()
                                  << " got EndOfStream  with nextPipeline");
                    } else if (auto* sink = std::get_if<DataSinkPtr>(&successorPipeline)) {
                        auto newReconf = ReconfigurationMessage(queryId, querySubPlanId, task.getType(), *sink);
                        queryManager->addReconfigurationMessage(queryId, querySubPlanId, newReconf, false);
                        NES_DEBUG("Going to reconfigure next sink belonging to subplanId: "
                                  << querySubPlanId << " sink id: " << (*sink)->toString()
                                  << " got EndOfStream  with nextPipeline");
                    }
                }

            } else {
                NES_DEBUG("Requested reconfiguration of pipeline belonging to subplanId: "
                          << querySubPlanId << " stage id: " << pipelineId << " but refCount was " << (prevProducerCounter)
                          << " and now is " << (prevProducerCounter - 1));
            }
            break;
        }
        default: {
            break;
        }
    }
}

}// namespace NES::Runtime::Execution
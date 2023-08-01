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

#include <Network/NetworkSink.hpp>
#include <Network/NetworkSource.hpp>
#include <Runtime/AsyncTaskExecutor.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger//Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <memory>
#include <variant>

namespace NES::Runtime {
void AbstractQueryManager::notifyQueryStatusChange(const Execution::ExecutableQueryPlanPtr& qep,
                                                   Execution::ExecutableQueryPlanStatus status) {
    NES_ASSERT(qep, "Invalid query plan object");
    if (status == Execution::ExecutableQueryPlanStatus::Finished) {
        for (const auto& source : qep->getSources()) {
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(Runtime::QueryTerminationType::Graceful),
                                "Cannot cleanup source " << source->getOperatorId());// just a clean-up op
            }
        }
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            false);

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(),
                                                     qep->getQuerySubPlanId(),
                                                     Execution::ExecutableQueryPlanStatus::Finished);

    } else if (status == Execution::ExecutableQueryPlanStatus::ErrorState) {
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            false);

        queryStatusListener->notifyQueryStatusChange(qep->getQueryId(),
                                                     qep->getQuerySubPlanId(),
                                                     Execution::ExecutableQueryPlanStatus::ErrorState);
    }
}
void AbstractQueryManager::notifySourceFailure(DataSourcePtr failedSource, const std::string reason) {
    NES_DEBUG("notifySourceFailure called for query id=" << failedSource->getOperatorId() << " reason=" << reason);
    std::vector<Execution::ExecutableQueryPlanPtr> plansToFail;
    {
        std::unique_lock lock(queryMutex);
        plansToFail = sourceToQEPMapping[failedSource->getOperatorId()];
    }
    // we cant fail a query from a source because failing a query eventually calls stop on the failed query
    // this means we are going to call join on the source thread
    // however, notifySourceFailure may be called from the source thread itself, thus, resulting in a deadlock
    for (auto qepToFail : plansToFail) {
        auto future =
            asyncTaskExecutor->runAsync([this, reason, qepToFail = std::move(qepToFail)]() -> Execution::ExecutableQueryPlanPtr {
                NES_DEBUG("Going to fail query id=" << qepToFail->getQuerySubPlanId()
                                                    << " subplan=" << qepToFail->getQuerySubPlanId());
                if (failQuery(qepToFail)) {
                    NES_DEBUG("Failed query id=" << qepToFail->getQuerySubPlanId()
                                                 << " subplan=" << qepToFail->getQuerySubPlanId());
                    queryStatusListener->notifyQueryFailure(qepToFail->getQueryId(), qepToFail->getQuerySubPlanId(), reason);
                    return qepToFail;
                }
                return nullptr;
            });
    }
}

void AbstractQueryManager::notifyTaskFailure(Execution::SuccessorExecutablePipeline pipelineOrSink,
                                             const std::string& errorMessage) {

    QuerySubPlanId planId = 0;
    Execution::ExecutableQueryPlanPtr qepToFail;
    if (auto* pipe = std::get_if<Execution::ExecutablePipelinePtr>(&pipelineOrSink)) {
        planId = (*pipe)->getQuerySubPlanId();
    } else if (auto* sink = std::get_if<DataSinkPtr>(&pipelineOrSink)) {
        planId = (*sink)->getParentPlanId();
    }
    {
        std::unique_lock lock(queryMutex);
        if (auto it = runningQEPs.find(planId); it != runningQEPs.end()) {
            qepToFail = it->second;
        } else {
            NES_WARNING("Cannot fail non existing sub query plan: " << planId);
            return;
        }
    }
    auto future = asyncTaskExecutor->runAsync(
        [this, errorMessage](Execution::ExecutableQueryPlanPtr qepToFail) -> Execution::ExecutableQueryPlanPtr {
            NES_DEBUG("Going to fail query id=" << qepToFail->getQuerySubPlanId()
                                                << " subplan=" << qepToFail->getQuerySubPlanId());
            if (failQuery(qepToFail)) {
                NES_DEBUG("Failed query id=" << qepToFail->getQuerySubPlanId() << " subplan=" << qepToFail->getQuerySubPlanId());
                queryStatusListener->notifyQueryFailure(qepToFail->getQueryId(), qepToFail->getQuerySubPlanId(), errorMessage);
                return qepToFail;
            }
            return nullptr;
        },
        std::move(qepToFail));
}

void AbstractQueryManager::notifySourceCompletion(DataSourcePtr source, QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    //THIS is now shutting down all
    for (auto& entry : sourceToQEPMapping[source->getOperatorId()]) {
        NES_TRACE("notifySourceCompletion operator id=" << source->getOperatorId() << "plan id=" << entry->getQueryId()
                                                        << " subplan=" << entry->getQuerySubPlanId());
        entry->notifySourceCompletion(source, terminationType);
        if (terminationType == QueryTerminationType::Graceful) {
            queryStatusListener->notifySourceTermination(entry->getQueryId(),
                                                         entry->getQuerySubPlanId(),
                                                         source->getOperatorId(),
                                                         QueryTerminationType::Graceful);
        }
    }
}

void AbstractQueryManager::notifyPipelineCompletion(QuerySubPlanId subPlanId,
                                                    Execution::ExecutablePipelinePtr pipeline,
                                                    QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan for pipeline " << pipeline->getPipelineId());
    qep->notifyPipelineCompletion(pipeline, terminationType);
}

void AbstractQueryManager::notifySinkCompletion(QuerySubPlanId subPlanId,
                                                DataSinkPtr sink,
                                                QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    auto& qep = runningQEPs[subPlanId];
    NES_ASSERT2_FMT(qep, "invalid query plan " << subPlanId << " for sink " << sink->toString());
    qep->notifySinkCompletion(sink, terminationType);
}
}// namespace NES::Runtime
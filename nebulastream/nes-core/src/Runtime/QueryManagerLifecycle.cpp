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
#include <stack>
#include <utility>

namespace NES::Runtime {
bool AbstractQueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("AbstractQueryManager::registerQueryInNodeEngine: query" << qep->getQueryId()
                                                                       << " subquery=" << qep->getQuerySubPlanId());

    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "AbstractQueryManager::registerQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                                       << qep->getQueryId());
    {
        std::scoped_lock lock(queryMutex);
        // test if elements already exist
        NES_DEBUG("AbstractQueryManager: resolving sources for query " << qep << " with sources=" << qep->getSources().size());

        for (const auto& source : qep->getSources()) {
            // source already exists, add qep to source set if not there
            OperatorId sourceOperatorId = source->getOperatorId();

            NES_DEBUG("AbstractQueryManager: Source " << sourceOperatorId << " not found. Creating new element with with qep ");
            //If source sharing is active and this is the first query for this source, init the map
            if (sourceToQEPMapping.find(sourceOperatorId) == sourceToQEPMapping.end()) {
                sourceToQEPMapping[sourceOperatorId] = std::vector<Execution::ExecutableQueryPlanPtr>();
            }

            //bookkeep which qep is now reading from this source
            sourceToQEPMapping[sourceOperatorId].push_back(qep);
        }
    }

    NES_DEBUG("queryToStatisticsMap add for=" << qep->getQuerySubPlanId() << " pair queryId=" << qep->getQueryId()
                                              << " subplanId=" << qep->getQuerySubPlanId());

    //TODO: Tiis assumes 1) that there is only one pipeline per query and 2) that the subqueryplan id is unique => both can become a problem
    queryToStatisticsMap.insert(qep->getQuerySubPlanId(),
                                std::make_shared<QueryStatistics>(qep->getQueryId(), qep->getQuerySubPlanId()));

    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "AbstractQueryManager::startQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                                    << qep->getQueryId());
    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Created,
               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

    // 1. start the qep and handlers, if any
    if (!qep->setup() || !qep->start(std::move(stateManager))) {
        NES_FATAL_ERROR("AbstractQueryManager: query execution plan could not started");
        return false;
    }

    std::vector<AsyncTaskExecutor::AsyncTaskFuture<bool>> startFutures;

    std::vector<Network::NetworkSourcePtr> netSources;
    std::vector<Network::NetworkSinkPtr> netSinks;

    // 2a. sort net sinks and sources
    for (const auto& sink : qep->getSinks()) {
        if (auto netSink = std::dynamic_pointer_cast<Network::NetworkSink>(sink); netSink) {
            netSinks.emplace_back(netSink);
        }
    }
    std::sort(netSinks.begin(), netSinks.end(), [](const Network::NetworkSinkPtr& lhs, const Network::NetworkSinkPtr& rhs) {
        return *lhs < *rhs;
    });
    for (const auto& sink : qep->getSources()) {
        if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(sink); netSource) {
            netSources.emplace_back(netSource);
        }
    }
    std::sort(netSources.begin(),
              netSources.end(),
              [](const Network::NetworkSourcePtr& lhs, const Network::NetworkSourcePtr& rhs) {
                  return *lhs < *rhs;
              });

    // 2b. pre-start net sinks
    for (const auto& netSink : netSinks) {
        netSink->preSetup();
    }

    // 2b. start net sinks
    for (const auto& sink : netSinks) {
        startFutures.emplace_back(asyncTaskExecutor->runAsync([sink]() {
            NES_DEBUG("AbstractQueryManager: start network sink " << sink->toString());
            sink->setup();
            return true;
        }));
    }

    // 3a. pre-start net sources
    for (const auto& source : netSources) {
        if (!source->bind()) {
            NES_WARNING("AbstractQueryManager: network source " << source << " could not started as it is already running");
        } else {
            NES_DEBUG("AbstractQueryManager: network source " << source << " started successfully");
        }
    }

    // 3b. start net sources
    for (const auto& source : netSources) {
        if (!source->start()) {
            NES_WARNING("AbstractQueryManager: network source " << source << " could not started as it is already running");
        } else {
            NES_DEBUG("AbstractQueryManager: network source " << source << " started successfully");
        }
    }

    for (auto& future : startFutures) {
        NES_ASSERT(future.wait(), "Cannot start query");
    }

    // 4. start data sinks
    for (const auto& sink : qep->getSinks()) {
        if (std::dynamic_pointer_cast<Network::NetworkSink>(sink)) {
            continue;
        }
        NES_DEBUG("AbstractQueryManager: start sink " << sink->toString());
        sink->setup();
    }

    {
        std::unique_lock lock(queryMutex);
        runningQEPs.emplace(qep->getQuerySubPlanId(), qep);
    }

    return true;
}

bool MultiQueueQueryManager::registerQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    auto ret = AbstractQueryManager::registerQuery(qep);
    std::scoped_lock lock(queryMutex);
    NES_ASSERT2_FMT(queryToStatisticsMap.size() <= numberOfQueues,
                    "AbstractQueryManager::registerQuery: not enough queues are free for numberOfQueues="
                        << numberOfQueues << " query cnt=" << queryToStatisticsMap.size());
    //currently we asume all queues have same number of threads so we can do this.
    queryToTaskQueueIdMap[qep->getQueryId()] = currentTaskQueueId++;
    NES_DEBUG("queryToTaskQueueIdMap add for=" << qep->getQueryId() << " queue=" << currentTaskQueueId - 1);
    return ret;
}

bool AbstractQueryManager::startQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("AbstractQueryManager::startQuery: query id " << qep->getQuerySubPlanId() << " " << qep->getQueryId());
    NES_ASSERT2_FMT(queryManagerStatus.load() == Running,
                    "AbstractQueryManager::startQuery: cannot accept new query id " << qep->getQuerySubPlanId() << " "
                                                                                    << qep->getQueryId());
    //    NES_ASSERT(qep->getStatus() == Execution::ExecutableQueryPlanStatus::Running,
    //               "Invalid status for starting the QEP " << qep->getQuerySubPlanId());

    // 5. start data sources
    for (const auto& source : qep->getSources()) {
        if (std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
            continue;
        }
        NES_DEBUG("AbstractQueryManager: start source " << source << " str=" << source->toString());
        if (!source->start()) {
            NES_WARNING("AbstractQueryManager: source " << source << " could not started as it is already running");
        } else {
            NES_DEBUG("AbstractQueryManager: source " << source << " started successfully");
        }
    }

    // register start timestamp of query in statistics
    if (queryToStatisticsMap.contains(qep->getQuerySubPlanId())) {
        auto statistics = queryToStatisticsMap.find(qep->getQuerySubPlanId());
        auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();
        statistics->setTimestampQueryStart(now, true);
    } else {
        NES_FATAL_ERROR("queryToStatisticsMap not set, this should only happen for testing");
        NES_THROW_RUNTIME_ERROR("got buffer for not registered qep");
    }

    return true;
}

bool AbstractQueryManager::deregisterQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    NES_DEBUG("AbstractQueryManager::deregisterAndUndeployQuery: query" << qep->getQueryId());
    std::unique_lock lock(queryMutex);
    auto qepStatus = qep->getStatus();
    NES_ASSERT2_FMT(qepStatus == Execution::ExecutableQueryPlanStatus::Finished
                        || qepStatus == Execution::ExecutableQueryPlanStatus::ErrorState
                        || qepStatus == Execution::ExecutableQueryPlanStatus::Stopped,
                    "Cannot deregisterQuery query " << qep->getQuerySubPlanId() << " as it is not stopped/failed");
    for (auto const& source : qep->getSources()) {
        sourceToQEPMapping.erase(source->getOperatorId());
    }
    return true;
}

bool AbstractQueryManager::canTriggerEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType terminationType) {
    std::unique_lock lock(queryMutex);
    NES_ASSERT2_FMT(sourceToQEPMapping.contains(source->getOperatorId()),
                    "source=" << source->getOperatorId() << " wants to terminate but is not part of the mapping process");

    bool overallResult = true;
    //we have to check for each query on this source if we can terminate and return a common answer
    for (auto qep : sourceToQEPMapping[source->getOperatorId()]) {
        bool ret = queryStatusListener->canTriggerEndOfStream(qep->getQueryId(),
                                                              qep->getQuerySubPlanId(),
                                                              source->getOperatorId(),
                                                              terminationType);
        if (!ret) {
            NES_ERROR("Query cannot trigger EOS for query manager for query =" << qep->getQueryId());
            NES_THROW_RUNTIME_ERROR("cannot trigger EOS in canTriggerEndOfStream()");
        }
        overallResult &= ret;
    }
    return overallResult;
}

bool AbstractQueryManager::failQuery(const Execution::ExecutableQueryPlanPtr& qep) {
    bool ret = true;
    NES_DEBUG("AbstractQueryManager::failQuery: query=" << qep->getQueryId());
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQuerySubPlanId());
            break;
        }
        default: {
            break;
        }
    }
    for (const auto& source : qep->getSources()) {
        NES_ASSERT2_FMT(source->fail(),
                        "Cannot fail source " << source->getOperatorId()
                                              << " belonging to query plan=" << qep->getQuerySubPlanId());
    }

    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Fail) {
                NES_FATAL_ERROR("AbstractQueryManager: QEP " << qep->getQuerySubPlanId() << " could not be failed");
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline getQuerySubPlanId()=" << qep->getQuerySubPlanId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            true);
    }
    NES_DEBUG("AbstractQueryManager::failQuery: query " << qep->getQuerySubPlanId() << " was "
                                                        << (ret ? "successful" : " not successful"));
    return true;
}

bool AbstractQueryManager::stopQuery(const Execution::ExecutableQueryPlanPtr& qep, Runtime::QueryTerminationType type) {
    NES_DEBUG("AbstractQueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " type=" << type);
    NES_ASSERT2_FMT(type != QueryTerminationType::Failure, "Requested stop with failure for query " << qep->getQuerySubPlanId());
    bool ret = true;
    switch (qep->getStatus()) {
        case Execution::ExecutableQueryPlanStatus::ErrorState:
        case Execution::ExecutableQueryPlanStatus::Finished:
        case Execution::ExecutableQueryPlanStatus::Stopped: {
            NES_DEBUG("AbstractQueryManager::stopQuery: query sub-plan id " << qep->getQuerySubPlanId() << " is already stopped");
            return true;
        }
        case Execution::ExecutableQueryPlanStatus::Invalid: {
            NES_ASSERT2_FMT(false, "not supported yet " << qep->getQuerySubPlanId());
            break;
        }
        default: {
            break;
        }
    }

    for (const auto& source : qep->getSources()) {
        if (type == QueryTerminationType::Graceful) {
            // graceful shutdown :: only leaf sources
            if (!std::dynamic_pointer_cast<Network::NetworkSource>(source)) {
                NES_ASSERT2_FMT(source->stop(QueryTerminationType::Graceful),
                                "Cannot terminate source " << source->getOperatorId());
            }
        } else if (type == QueryTerminationType::HardStop) {
            NES_ASSERT2_FMT(source->stop(QueryTerminationType::HardStop), "Cannot terminate source " << source->getOperatorId());
        }
    }

    if (type == QueryTerminationType::HardStop || type == QueryTerminationType::Failure) {
        for (auto& stage : qep->getPipelines()) {
            NES_ASSERT2_FMT(stage->stop(type), "Cannot hard stop pipeline " << stage->getPipelineId());
        }
    }

    // TODO evaluate if we need to have this a wait instead of a get
    // TODO for instance we could wait N seconds and if the stopped is not succesful by then
    // TODO we need to trigger a hard local kill of a QEP
    auto terminationFuture = qep->getTerminationFuture();
    auto terminationStatus = terminationFuture.wait_for(std::chrono::minutes(10));
    switch (terminationStatus) {
        case std::future_status::ready: {
            if (terminationFuture.get() != Execution::ExecutableQueryPlanResult::Ok) {
                NES_FATAL_ERROR("AbstractQueryManager: QEP " << qep->getQuerySubPlanId() << " could not be stopped");
                ret = false;
            }
            break;
        }
        case std::future_status::timeout:
        case std::future_status::deferred: {
            // TODO we need to fail the query now as it could not be stopped?
            NES_ASSERT2_FMT(false, "Cannot stop query within deadline " << qep->getQuerySubPlanId());
            break;
        }
    }
    if (ret) {
        addReconfigurationMessage(
            qep->getQueryId(),
            qep->getQuerySubPlanId(),
            ReconfigurationMessage(qep->getQueryId(), qep->getQuerySubPlanId(), Destroy, inherited1::shared_from_this()),
            true);
    }
    NES_DEBUG("AbstractQueryManager::stopQuery: query " << qep->getQuerySubPlanId() << " was "
                                                        << (ret ? "successful" : " not successful"));
    return ret;
}

bool AbstractQueryManager::addSoftEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    // send EOS to `source` itself, iff a network source
    if (auto netSource = std::dynamic_pointer_cast<Network::NetworkSource>(source); netSource != nullptr) {
        //add soft eaos for network source
        auto reconfMessage = ReconfigurationMessage(-1, -1, SoftEndOfStream, netSource);
        addReconfigurationMessage(-1, -1, reconfMessage, false);
    }

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                                        executablePipeline->get()->getQuerySubPlanId(),
                                                        SoftEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                      executablePipeline->get()->getQuerySubPlanId(),
                                      reconfMessage,
                                      false);
            NES_DEBUG("soft end-of-stream Exec Pipeline opId="
                      << sourceId << " reconfType=" << int(SoftEndOfStream)
                      << " queryExecutionPlanId=" << executablePipeline->get()->getQuerySubPlanId()
                      << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep"
                      << executablePipeline->get()->getQueryId());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink =
                ReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), SoftEndOfStream, (*sink));
            addReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), reconfMessageSink, false);
            NES_DEBUG("soft end-of-stream Sink opId=" << sourceId << " reconfType=" << int(SoftEndOfStream)
                                                      << " queryExecutionPlanId=" << sink->get()->getParentPlanId()
                                                      << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads()
                                                      << " qep" << sink->get()->getQueryId());
        }
    }
    return true;
}

bool AbstractQueryManager::addHardEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                                        executablePipeline->get()->getQuerySubPlanId(),
                                                        HardEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                      executablePipeline->get()->getQuerySubPlanId(),
                                      reconfMessage,
                                      false);
            NES_DEBUG("hard end-of-stream Exec Op opId="
                      << sourceId << " reconfType=" << HardEndOfStream << " queryId=" << executablePipeline->get()->getQueryId()
                      << " querySubPlanId=" << executablePipeline->get()->getQuerySubPlanId()
                      << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads());
        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink =
                ReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), HardEndOfStream, (*sink));
            addReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), reconfMessageSink, false);
            NES_DEBUG("hard end-of-stream Sink opId="
                      << sourceId << " reconfType=" << HardEndOfStream << " queryId=" << sink->get()->getQueryId()
                      << " querySubPlanId=" << sink->get()->getParentPlanId()
                      << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads());
        }
    }

    return true;
}

bool AbstractQueryManager::addFailureEndOfStream(DataSourcePtr source) {
    auto sourceId = source->getOperatorId();
    auto pipelineSuccessors = source->getExecutableSuccessors();

    for (auto successor : pipelineSuccessors) {
        // create reconfiguration message. If the successor is a executable pipeline we send a reconfiguration message to the pipeline.
        // If successor is a data sink we send the reconfiguration message to the query plan.
        if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&successor)) {
            auto reconfMessage = ReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                                        executablePipeline->get()->getQuerySubPlanId(),
                                                        FailEndOfStream,
                                                        (*executablePipeline));
            addReconfigurationMessage(executablePipeline->get()->getQueryId(),
                                      executablePipeline->get()->getQuerySubPlanId(),
                                      reconfMessage,
                                      false);

            NES_DEBUG("failure end-of-stream Exec Op opId="
                      << sourceId << " reconfType=" << FailEndOfStream
                      << " queryExecutionPlanId=" << executablePipeline->get()->getQuerySubPlanId()
                      << " threadPool->getNumberOfThreads()=" << threadPool->getNumberOfThreads() << " qep"
                      << executablePipeline->get()->getQueryId());

        } else if (auto* sink = std::get_if<DataSinkPtr>(&successor)) {
            auto reconfMessageSink =
                ReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), FailEndOfStream, (*sink));
            addReconfigurationMessage(sink->get()->getQueryId(), sink->get()->getParentPlanId(), reconfMessageSink, false);
            NES_DEBUG("failure end-of-stream Sink opId="
                      << sourceId << " reconfType=" << FailEndOfStream
                      << " queryExecutionPlanId=" << sink->get()->getParentPlanId() << " threadPool->getNumberOfThreads()="
                      << threadPool->getNumberOfThreads() << " qep" << sink->get()->getQueryId());
        }
    }
    return true;
}

bool AbstractQueryManager::addEpochPropagation(DataSourcePtr source, uint64_t queryId, uint64_t epochBarrier) {
    std::unique_lock lock(queryMutex);
    auto qeps = sourceToQEPMapping.find(source->getOperatorId());
    if (qeps != sourceToQEPMapping.end()) {
        //post reconfiguration message to the executable query plans with an epoch barrier to trim buffer storages
        for (auto qep : qeps->second) {
            auto sinks = qep->getSinks();
            for (auto sink : sinks) {
                if (sink->getSinkMediumType() == SinkMediumTypes::NETWORK_SINK) {
                    auto newReconf = ReconfigurationMessage(queryId,
                                                            qep->getQuerySubPlanId(),
                                                            Runtime::ReconfigurationType::PropagateEpoch,
                                                            sink,
                                                            std::make_any<uint64_t>(epochBarrier));
                    addReconfigurationMessage(queryId, qep->getQuerySubPlanId(), newReconf);
                }
            }
            return true;
        }
    } else {
        NES_THROW_RUNTIME_ERROR("AbstractQueryManager: no query execution plans were found");
    }
    return false;
}

bool AbstractQueryManager::addEndOfStream(DataSourcePtr source, Runtime::QueryTerminationType terminationType) {
    std::unique_lock queryLock(queryMutex);
    NES_DEBUG("AbstractQueryManager: AbstractQueryManager::addEndOfStream for source operator "
              << source->getOperatorId() << " terminationType=" << terminationType);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool no longer running");
    NES_ASSERT2_FMT(sourceToQEPMapping.contains(source->getOperatorId()), "invalid source " << source->getOperatorId());

    bool success = false;
    switch (terminationType) {
        case Runtime::QueryTerminationType::Graceful: {
            success = addSoftEndOfStream(source);
            break;
        }
        case Runtime::QueryTerminationType::HardStop: {
            success = addHardEndOfStream(source);
            break;
        }
        case Runtime::QueryTerminationType::Failure: {
            success = addFailureEndOfStream(source);
            break;
        }
        default: {
            NES_ASSERT2_FMT(false, "Invalid termination type");
        }
    }

    return success;
}

}// namespace NES::Runtime
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

namespace detail {

class ReconfigurationPipelineExecutionContext : public Execution::PipelineExecutionContext {
  public:
    explicit ReconfigurationPipelineExecutionContext(QuerySubPlanId queryExecutionPlanId, QueryManagerPtr queryManager)
        : Execution::PipelineExecutionContext(
            -1,// this is a dummy pipelineID
            queryExecutionPlanId,
            queryManager->getBufferManager(),
            queryManager->getNumberOfWorkerThreads(),
            [](TupleBuffer&, NES::Runtime::WorkerContext&) {
            },
            [](TupleBuffer&) {
            },
            std::vector<Execution::OperatorHandlerPtr>()) {
        // nop
    }
};

class ReconfigurationEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
    using base = Execution::ExecutablePipelineStage;

  public:
    explicit ReconfigurationEntryPointPipelineStage() : base(Unary) {
        // nop
    }

    ExecutionResult execute(TupleBuffer& buffer, Execution::PipelineExecutionContext&, WorkerContextRef workerContext) {
        NES_TRACE(
            "QueryManager: AbstractQueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint begin on thread "
            << workerContext.getId());
        auto* task = buffer.getBuffer<ReconfigurationMessage>();
        NES_TRACE("QueryManager: AbstractQueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to wait "
                  "on thread "
                  << workerContext.getId());
        task->wait();
        NES_TRACE(
            "QueryManager: AbstractQueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint going to reconfigure "
            "on thread "
            << workerContext.getId());
        task->getInstance()->reconfigure(*task, workerContext);
        NES_TRACE("QueryManager: AbstractQueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint post callback "
                  "on thread "
                  << workerContext.getId());
        task->postReconfiguration();
        NES_TRACE(
            "QueryManager: AbstractQueryManager::addReconfigurationMessage ReconfigurationMessageEntryPoint completed on thread "
            << workerContext.getId());
        task->postWait();
        return ExecutionResult::Ok;
    }
};
}// namespace detail

ExecutionResult DynamicQueryManager::processNextTask(bool running, WorkerContext& workerContext) {
    NES_TRACE("QueryManager: AbstractQueryManager::getWork wait get lock");
    Task task;
    if (running) {
        taskQueue.blockingRead(task);

#ifdef ENABLE_PAPI_PROFILER
        auto profiler = cpuProfilers[NesThread::getId() % cpuProfilers.size()];
        auto numOfInputTuples = task.getNumberOfInputTuples();
        profiler->startSampling();
#endif

        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        ExecutionResult result = task(workerContext);
#ifdef ENABLE_PAPI_PROFILER
        profiler->stopSampling(numOfInputTuples);
#endif

        switch (result) {
            case ExecutionResult::Ok: {
                completedWork(task, workerContext);
                return ExecutionResult::Ok;
            }
            default: {
                return result;
            }
        }
    } else {
        return terminateLoop(workerContext);
    }
}

ExecutionResult MultiQueueQueryManager::processNextTask(bool running, WorkerContext& workerContext) {
    NES_TRACE("QueryManager: AbstractQueryManager::getWork wait get lock");
    Task task;
    if (running) {
        taskQueues[workerContext.getQueueId()].blockingRead(task);

#ifdef ENABLE_PAPI_PROFILER
        auto profiler = cpuProfilers[NesThread::getId() % cpuProfilers.size()];
        auto numOfInputTuples = task.getNumberOfInputTuples();
        profiler->startSampling();
#endif

        NES_TRACE("QueryManager: provide task" << task.toString() << " to thread (getWork())");
        auto result = task(workerContext);
#ifdef ENABLE_PAPI_PROFILER
        profiler->stopSampling(numOfInputTuples);
#endif

        switch (result) {
            case ExecutionResult::Ok: {
                completedWork(task, workerContext);
                return ExecutionResult::Ok;
            }
            default: {
                return result;
            }
        }
    } else {
        return terminateLoop(workerContext);
    }
}
ExecutionResult DynamicQueryManager::terminateLoop(WorkerContext& workerContext) {
    bool hitReconfiguration = false;
    Task task;
    while (taskQueue.read(task)) {
        if (!hitReconfiguration) {// execute all pending tasks until first reconfiguration
            task(workerContext);
            if (task.isReconfiguration()) {
                hitReconfiguration = true;
            }
        } else {
            if (task.isReconfiguration()) {// execute only pending reconfigurations
                task(workerContext);
            }
        }
    }
    return ExecutionResult::Finished;
}

void DynamicQueryManager::addWorkForNextPipeline(TupleBuffer& buffer,
                                                 Execution::SuccessorExecutablePipeline executable,
                                                 uint32_t queueId) {
    NES_TRACE("Add Work for executable for queue=" << queueId);
    if (auto nextPipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable); nextPipeline) {
        if (!(*nextPipeline)->isRunning()) {
            // we ignore task if the pipeline is not running anymore.
            NES_WARNING("Pushed task for non running executable pipeline id=" << (*nextPipeline)->getPipelineId());
            return;
        }
        NES_TRACE("QueryManager: added Task this pipelineID="
                  << (*nextPipeline)->getPipelineId() << "  for Number of next pipelines "
                  << (*nextPipeline)->getSuccessors().size() << " inputBuffer " << buffer
                  << " queryId=" << (*nextPipeline)->getQueryId() << " getQuerySubPlanId=" << (*nextPipeline)->getQuerySubPlanId()
                  << " queueId=" << queueId);

        taskQueue.blockingWrite(Task(executable, buffer, getNextTaskId()));

    } else if (auto sink = std::get_if<DataSinkPtr>(&executable); sink) {
        NES_TRACE("QueryManager: added Task for Sink " << sink->get()->toString() << " inputBuffer " << buffer
                                                       << " queueId=" << queueId);

        taskQueue.blockingWrite(Task(executable, buffer, getNextTaskId()));
    } else {
        NES_THROW_RUNTIME_ERROR("This should not happen");
    }
}

ExecutionResult MultiQueueQueryManager::terminateLoop(WorkerContext& workerContext) {
    bool hitReconfiguration = false;
    Task task;
    while (taskQueues[workerContext.getQueueId()].read(task)) {
        if (!hitReconfiguration) {// execute all pending tasks until first reconfiguration
            task(workerContext);
            if (task.isReconfiguration()) {
                hitReconfiguration = true;
            }
        } else {
            if (task.isReconfiguration()) {// execute only pending reconfigurations
                task(workerContext);
            }
        }
    }
    return ExecutionResult::Finished;
}

void MultiQueueQueryManager::addWorkForNextPipeline(TupleBuffer& buffer,
                                                    Execution::SuccessorExecutablePipeline executable,
                                                    uint32_t queueId) {
    NES_TRACE("Add Work for executable for queue=" << queueId);
    NES_ASSERT(queueId < taskQueues.size(), "Invalid queue id");
    if (auto nextPipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        if (!(*nextPipeline)->isRunning()) {
            // we ignore task if the pipeline is not running anymore.
            NES_WARNING("Pushed task for non running executable pipeline id=" << (*nextPipeline)->getPipelineId());
            return;
        }
        NES_TRACE("QueryManager: added Task this pipelineID="
                  << (*nextPipeline)->getPipelineId() << "  for Number of next pipelines "
                  << (*nextPipeline)->getSuccessors().size() << " inputBuffer " << buffer << " queueId=" << queueId);

        taskQueues[queueId].write(Task(executable, buffer, getNextTaskId()));
    } else if (auto sink = std::get_if<DataSinkPtr>(&executable)) {
        NES_TRACE("QueryManager: added Task for Sink " << sink->get()->toString() << " inputBuffer " << buffer
                                                       << " queueId=" << queueId);

        taskQueues[queueId].write(Task(executable, buffer, getNextTaskId()));
    } else {
        NES_THROW_RUNTIME_ERROR("This should not happen");
    }
}

void DynamicQueryManager::updateStatistics(const Task& task,
                                           QueryId queryId,
                                           QuerySubPlanId querySubPlanId,
                                           WorkerContext& workerContext) {
    AbstractQueryManager::updateStatistics(task, queryId, querySubPlanId, workerContext);
#ifndef LIGHT_WEIGHT_STATISTICS
    if (queryToStatisticsMap.contains(querySubPlanId)) {
        auto statistics = queryToStatisticsMap.find(querySubPlanId);
        // with multiple queryIdAndCatalogEntryMapping this wont be correct
        auto qSize = taskQueue.size();
        statistics->incQueueSizeSum(qSize > 0 ? qSize : 0);
    }
#endif
}

void MultiQueueQueryManager::updateStatistics(const Task& task,
                                              QueryId queryId,
                                              QuerySubPlanId querySubPlanId,
                                              WorkerContext& workerContext) {
    AbstractQueryManager::updateStatistics(task, queryId, querySubPlanId, workerContext);
#ifndef LIGHT_WEIGHT_STATISTICS
    if (queryToStatisticsMap.contains(querySubPlanId)) {
        auto statistics = queryToStatisticsMap.find(querySubPlanId);
        auto qSize = taskQueues[workerContext.getQueueId()].size();
        statistics->incQueueSizeSum(qSize > 0 ? qSize : 0);
    }
#endif
}

void AbstractQueryManager::updateStatistics(const Task& task,
                                            QueryId queryId,
                                            QuerySubPlanId querySubPlanId,
                                            WorkerContext& workerContext) {
    tempCounterTasksCompleted[workerContext.getId() % tempCounterTasksCompleted.size()].fetch_add(1);
#ifndef LIGHT_WEIGHT_STATISTICS
    if (queryToStatisticsMap.contains(querySubPlanId)) {
        auto statistics = queryToStatisticsMap.find(querySubPlanId);

        auto now =
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch())
                .count();

        statistics->setTimestampFirstProcessedTask(now, true);
        statistics->setTimestampLastProcessedTask(now);
        statistics->incProcessedTasks();
        statistics->incProcessedBuffers();
        auto creation = task.getBufferRef().getCreationTimestamp();
        auto diff = now - creation;
        NES_ASSERT(creation <= (unsigned long) now, "timestamp is in the past");
        statistics->incLatencySum(diff);

        for (auto& bufferManager : bufferManagers) {
            statistics->incAvailableGlobalBufferSum(bufferManager->getAvailableBuffers());
            statistics->incAvailableFixedBufferSum(bufferManager->getAvailableBuffersInFixedSizePools());
        }

#ifdef NES_BENCHMARKS_DETAILED_LATENCY_MEASUREMENT
        statistics->addTimestampToLatencyValue(now, diff);
#endif
        statistics->incProcessedTuple(task.getNumberOfInputTuples());
    } else {
        using namespace std::string_literals;

        NES_ERROR("queryToStatisticsMap not set for "s + std::to_string(queryId) + " this should only happen for testing");
    }
#endif
}

void AbstractQueryManager::completedWork(Task& task, WorkerContext& wtx) {
    NES_TRACE("AbstractQueryManager::completedWork: Work for task=" << task.toString() << "worker ctx id=" << wtx.getId());
    if (task.isReconfiguration()) {
        return;
    }

    QuerySubPlanId querySubPlanId = -1;
    QueryId queryId = -1;
    auto executable = task.getExecutable();
    if (auto* sink = std::get_if<DataSinkPtr>(&executable)) {
        querySubPlanId = (*sink)->getParentPlanId();
        queryId = (*sink)->getQueryId();
        NES_TRACE("AbstractQueryManager::completedWork: task for sink querySubPlanId=" << querySubPlanId);
    } else if (auto* executablePipeline = std::get_if<Execution::ExecutablePipelinePtr>(&executable)) {
        querySubPlanId = (*executablePipeline)->getQuerySubPlanId();
        queryId = (*executablePipeline)->getQueryId();
        NES_TRACE("AbstractQueryManager::completedWork: task for exec pipeline isreconfig="
                  << (*executablePipeline)->isReconfiguration());
    }
    updateStatistics(task, queryId, querySubPlanId, wtx);
}

bool MultiQueueQueryManager::addReconfigurationMessage(QueryId queryId,
                                                       QuerySubPlanId queryExecutionPlanId,
                                                       const ReconfigurationMessage& message,
                                                       bool blocking) {
    NES_DEBUG("QueryManager: AbstractQueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << message.getType());
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    new (buffer.getBuffer()) ReconfigurationMessage(message, numberOfThreadsPerQueue, blocking);
    return addReconfigurationMessage(queryId, queryExecutionPlanId, std::move(buffer), blocking);
}

bool DynamicQueryManager::addReconfigurationMessage(QueryId queryId,
                                                    QuerySubPlanId queryExecutionPlanId,
                                                    const ReconfigurationMessage& message,
                                                    bool blocking) {
    NES_DEBUG("QueryManager: AbstractQueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << int(message.getType()));
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(sizeof(ReconfigurationMessage));
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();
    new (buffer.getBuffer()) ReconfigurationMessage(message, threadPool->getNumberOfThreads(), blocking);// memcpy using copy ctor
    return addReconfigurationMessage(queryId, queryExecutionPlanId, std::move(buffer), blocking);
}

bool DynamicQueryManager::addReconfigurationMessage(QueryId queryId,
                                                    QuerySubPlanId queryExecutionPlanId,
                                                    TupleBuffer&& buffer,
                                                    bool blocking) {
    std::unique_lock reconfLock(reconfigurationMutex);
    auto* task = buffer.getBuffer<ReconfigurationMessage>();
    NES_DEBUG("QueryManager: AbstractQueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << task->getType());
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId, inherited0::shared_from_this());
    auto reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
    auto pipeline = Execution::ExecutablePipeline::create(-1,
                                                          queryId,
                                                          queryExecutionPlanId,
                                                          inherited0::shared_from_this(),
                                                          pipelineContext,
                                                          reconfigurationExecutable,
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);

    for (uint64_t threadId = 0; threadId < threadPool->getNumberOfThreads(); threadId++) {
        taskQueue.blockingWrite(Task(pipeline, buffer, getNextTaskId()));
    }

    reconfLock.unlock();
    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    //    }
    return true;
}

bool MultiQueueQueryManager::addReconfigurationMessage(QueryId queryId,
                                                       QuerySubPlanId queryExecutionPlanId,
                                                       TupleBuffer&& buffer,
                                                       bool blocking) {
    std::unique_lock reconfLock(reconfigurationMutex);
    auto* task = buffer.getBuffer<ReconfigurationMessage>();
    NES_DEBUG("QueryManager: AbstractQueryManager::addReconfigurationMessage begins on plan "
              << queryExecutionPlanId << " blocking=" << blocking << " type " << task->getType()
              << " to queue=" << queryToTaskQueueIdMap[queryId]);
    NES_ASSERT2_FMT(threadPool->isRunning(), "thread pool not running");
    auto pipelineContext =
        std::make_shared<detail::ReconfigurationPipelineExecutionContext>(queryExecutionPlanId, inherited0::shared_from_this());
    auto reconfigurationExecutable = std::make_shared<detail::ReconfigurationEntryPointPipelineStage>();
    auto pipeline = Execution::ExecutablePipeline::create(-1,
                                                          queryId,
                                                          queryExecutionPlanId,
                                                          inherited0::shared_from_this(),
                                                          pipelineContext,
                                                          reconfigurationExecutable,
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);

    for (uint64_t threadId = 0; threadId < numberOfThreadsPerQueue; threadId++) {
        taskQueues[queryToTaskQueueIdMap[queryId]].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
    }

    reconfLock.unlock();
    if (blocking) {
        task->postWait();
        task->postReconfiguration();
    }
    //    }
    return true;
}

namespace detail {
class PoisonPillEntryPointPipelineStage : public Execution::ExecutablePipelineStage {
    using base = Execution::ExecutablePipelineStage;

  public:
    explicit PoisonPillEntryPointPipelineStage() : base(Unary) {
        // nop
    }

    virtual ~PoisonPillEntryPointPipelineStage() = default;

    ExecutionResult execute(TupleBuffer&, Execution::PipelineExecutionContext&, WorkerContextRef) {
        return ExecutionResult::AllFinished;
    }
};
}// namespace detail

void DynamicQueryManager::poisonWorkers() {
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(1);// there is always one buffer manager
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(-1, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          -1,// any sub query plan
                                                          -1,
                                                          inherited0::shared_from_this(),
                                                          pipelineContext,
                                                          std::make_shared<detail::PoisonPillEntryPointPipelineStage>(),
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);
    for (auto u{0ul}; u < threadPool->getNumberOfThreads(); ++u) {
        NES_DEBUG("Add poison for queue=" << u);
        taskQueue.blockingWrite(Task(pipeline, buffer, getNextTaskId()));
    }
}

void MultiQueueQueryManager::poisonWorkers() {
    auto optBuffer = bufferManagers[0]->getUnpooledBuffer(1);// there is always one buffer manager
    NES_ASSERT(optBuffer, "invalid buffer");
    auto buffer = optBuffer.value();

    auto pipelineContext = std::make_shared<detail::ReconfigurationPipelineExecutionContext>(-1, inherited0::shared_from_this());
    auto pipeline = Execution::ExecutablePipeline::create(-1,// any query plan
                                                          -1,// any sub query plan
                                                          -1,
                                                          inherited0::shared_from_this(),
                                                          pipelineContext,
                                                          std::make_shared<detail::PoisonPillEntryPointPipelineStage>(),
                                                          1,
                                                          std::vector<Execution::SuccessorExecutablePipeline>(),
                                                          true);

    for (auto u{0ul}; u < taskQueues.size(); ++u) {
        for (auto i{0ul}; i < numberOfThreadsPerQueue; ++i) {
            NES_DEBUG("Add poision for queue=" << u << " and thread=" << i);
            taskQueues[u].blockingWrite(Task(pipeline, buffer, getNextTaskId()));
        }
    }
}

}// namespace NES::Runtime
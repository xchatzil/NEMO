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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/BenchmarkSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MaterializedViewSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/SenseSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/StaticDataSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/BenchmarkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/StaticDataSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlanIterator.hpp>
#include <QueryCompiler/Phases/Translations/DataSinkProvider.hpp>
#include <QueryCompiler/Phases/Translations/DefaultDataSourceProvider.hpp>
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
#include <QueryCompiler/Phases/Translations/LowerToExecutableQueryPlanPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/Execution/ExecutablePipeline.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/WindowHandler/BatchJoinOperatorHandler.hpp>
#include <string>
#include <utility>
#include <variant>

namespace NES::QueryCompilation {
LowerToExecutableQueryPlanPhase::LowerToExecutableQueryPlanPhase(DataSinkProviderPtr sinkProvider,
                                                                 DataSourceProviderPtr sourceProvider)
    : sinkProvider(std::move(sinkProvider)), sourceProvider(std::move(sourceProvider)){};

LowerToExecutableQueryPlanPhasePtr LowerToExecutableQueryPlanPhase::create(const DataSinkProviderPtr& sinkProvider,
                                                                           const DataSourceProviderPtr& sourceProvider) {
    return std::make_shared<LowerToExecutableQueryPlanPhase>(sinkProvider, sourceProvider);
}

Runtime::Execution::ExecutableQueryPlanPtr LowerToExecutableQueryPlanPhase::apply(const PipelineQueryPlanPtr& pipelineQueryPlan,
                                                                                  const Runtime::NodeEnginePtr& nodeEngine) {
    std::vector<DataSourcePtr> sources;
    std::vector<DataSinkPtr> sinks;
    std::vector<Runtime::Execution::ExecutablePipelinePtr> executablePipelines;
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline> pipelineToExecutableMap;
    //Process all pipelines recursively.
    auto sourcePipelines = pipelineQueryPlan->getSourcePipelines();
    for (const auto& pipeline : sourcePipelines) {
        processSource(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan, pipelineToExecutableMap);
    }

    return std::make_shared<Runtime::Execution::ExecutableQueryPlan>(pipelineQueryPlan->getQueryId(),
                                                                     pipelineQueryPlan->getQuerySubPlanId(),
                                                                     std::move(sources),
                                                                     std::move(sinks),
                                                                     std::move(executablePipelines),
                                                                     nodeEngine->getQueryManager(),
                                                                     nodeEngine->getBufferManager());
}
Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processSuccessor(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    // check if the particular pipeline already exist in the pipeline map.
    if (pipelineToExecutableMap.find(pipeline->getPipelineId()) != pipelineToExecutableMap.end()) {
        return pipelineToExecutableMap[pipeline->getPipelineId()];
    }

    if (pipeline->isSinkPipeline()) {
        auto executableSink = processSink(pipeline, sources, sinks, executablePipelines, nodeEngine, pipelineQueryPlan);
        pipelineToExecutableMap[pipeline->getPipelineId()] = executableSink;
        return executableSink;
    }
    if (pipeline->isOperatorPipeline()) {
        auto executablePipeline = processOperatorPipeline(pipeline,
                                                          sources,
                                                          sinks,
                                                          executablePipelines,
                                                          nodeEngine,
                                                          pipelineQueryPlan,
                                                          pipelineToExecutableMap);
        pipelineToExecutableMap[pipeline->getPipelineId()] = executablePipeline;
        return executablePipeline;
    }
    throw QueryCompilationException("The pipeline was of wrong type. It should be a sink pipeline or a operator pipeline");
}

void LowerToExecutableQueryPlanPhase::processSource(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    if (!pipeline->isSourcePipeline()) {
        NES_ERROR("This is not a source pipeline.");
        NES_ERROR(pipeline->getQueryPlan()->toString());
        throw QueryCompilationException("This is not a source pipeline.");
    }

    //Convert logical source descriptor to actual source descriptor
    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sourceOperator = rootOperator->as<PhysicalOperators::PhysicalSourceOperator>();
    auto sourceDescriptor = sourceOperator->getSourceDescriptor();
    if (sourceDescriptor->instanceOf<LogicalSourceDescriptor>()) {
        //Fetch logical and physical source name in the descriptor
        auto logicalSourceName = sourceDescriptor->getLogicalSourceName();
        auto physicalSourceName = sourceDescriptor->getPhysicalSourceName();
        //Iterate over all available physical sources
        bool foundPhysicalSource = false;
        for (const auto& physicalSource : nodeEngine->getPhysicalSources()) {
            //Check if logical and physical source name matches with any of the physical source provided by the node
            if (physicalSource->getLogicalSourceName() == logicalSourceName
                && physicalSource->getPhysicalSourceName() == physicalSourceName) {
                sourceDescriptor = createSourceDescriptor(sourceDescriptor->getSchema(), physicalSource);
                foundPhysicalSource = true;
                break;
            }
        }
        if (!foundPhysicalSource) {
            throw QueryCompilationException("Unable to find the Physical source with logical source name " + logicalSourceName
                                            + " and physical source name " + physicalSourceName);
        }
    }

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    pipelineQueryPlan,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto source = sourceProvider->lower(sourceOperator->getId(),
                                        sourceOperator->getOriginId(),
                                        sourceDescriptor,
                                        nodeEngine,
                                        executableSuccessorPipelines);

    // Add this source as a predecessor to the pipeline execution context's of all its children.
    // This way you can navigate upstream.
    for (auto executableSuccessor : executableSuccessorPipelines) {
        if (const auto* nextExecutablePipeline = std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
            NES_DEBUG("Adding current source operator: " << source->getOperatorId() << " as a predecessor to its child pipeline: "
                                                         << (*nextExecutablePipeline)->getPipelineId());
            (*nextExecutablePipeline)->getContext()->addPredecessor(source);
        }
        // note: we do not register predecessors for DataSinks.
    }
    sources.emplace_back(source);
}

Runtime::Execution::SuccessorExecutablePipeline
LowerToExecutableQueryPlanPhase::processSink(const OperatorPipelinePtr& pipeline,
                                             std::vector<DataSourcePtr>&,
                                             std::vector<DataSinkPtr>& sinks,
                                             std::vector<Runtime::Execution::ExecutablePipelinePtr>&,
                                             Runtime::NodeEnginePtr nodeEngine,
                                             const PipelineQueryPlanPtr& pipelineQueryPlan) {
    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto sinkOperator = rootOperator->as<PhysicalOperators::PhysicalSinkOperator>();
    auto numOfProducers = pipeline->getPredecessors().size();
    auto sink = sinkProvider->lower(sinkOperator->getId(),
                                    sinkOperator->getSinkDescriptor(),
                                    sinkOperator->getOutputSchema(),
                                    std::move(nodeEngine),
                                    pipelineQueryPlan,
                                    numOfProducers);
    sinks.emplace_back(sink);
    return sink;
}

Runtime::Execution::SuccessorExecutablePipeline LowerToExecutableQueryPlanPhase::processOperatorPipeline(
    const OperatorPipelinePtr& pipeline,
    std::vector<DataSourcePtr>& sources,
    std::vector<DataSinkPtr>& sinks,
    std::vector<Runtime::Execution::ExecutablePipelinePtr>& executablePipelines,
    const Runtime::NodeEnginePtr& nodeEngine,
    const PipelineQueryPlanPtr& pipelineQueryPlan,
    std::map<uint64_t, Runtime::Execution::SuccessorExecutablePipeline>& pipelineToExecutableMap) {

    auto rootOperator = pipeline->getQueryPlan()->getRootOperators()[0];
    auto executableOperator = rootOperator->as<ExecutableOperator>();

    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessorPipelines;
    for (const auto& successor : pipeline->getSuccessors()) {
        auto executableSuccessor = processSuccessor(successor,
                                                    sources,
                                                    sinks,
                                                    executablePipelines,
                                                    nodeEngine,
                                                    pipelineQueryPlan,
                                                    pipelineToExecutableMap);
        executableSuccessorPipelines.emplace_back(executableSuccessor);
    }

    auto queryManager = nodeEngine->getQueryManager();

    auto emitToSuccessorFunctionHandler = [executableSuccessorPipelines](Runtime::TupleBuffer& buffer,
                                                                         Runtime::WorkerContextRef workerContext) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            if (const auto* sink = std::get_if<DataSinkPtr>(&executableSuccessor)) {
                NES_TRACE("Emit Buffer to data sink " << (*sink)->toString());
                (*sink)->writeData(buffer, workerContext);
            } else if (const auto* nextExecutablePipeline =
                           std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
                NES_TRACE("Emit Buffer to pipeline" << (*nextExecutablePipeline)->getPipelineId());
                (*nextExecutablePipeline)->execute(buffer, workerContext);
            }
        }
    };

    auto emitToQueryManagerFunctionHandler = [executableSuccessorPipelines, queryManager](Runtime::TupleBuffer& buffer) {
        for (const auto& executableSuccessor : executableSuccessorPipelines) {
            NES_DEBUG("Emit buffer to query manager");
            queryManager->addWorkForNextPipeline(buffer, executableSuccessor);
        }
    };

    auto executionContext =
        std::make_shared<Runtime::Execution::PipelineExecutionContext>(pipeline->getPipelineId(),
                                                                       pipelineQueryPlan->getQuerySubPlanId(),
                                                                       queryManager->getBufferManager(),
                                                                       queryManager->getNumberOfWorkerThreads(),
                                                                       emitToSuccessorFunctionHandler,
                                                                       emitToQueryManagerFunctionHandler,
                                                                       executableOperator->getOperatorHandlers());

    auto executablePipeline = Runtime::Execution::ExecutablePipeline::create(pipeline->getPipelineId(),
                                                                             pipelineQueryPlan->getQueryId(),
                                                                             pipelineQueryPlan->getQuerySubPlanId(),
                                                                             queryManager,
                                                                             executionContext,
                                                                             executableOperator->getExecutablePipelineStage(),
                                                                             pipeline->getPredecessors().size(),
                                                                             executableSuccessorPipelines);

    // Add this pipeline as a predecessor to the pipeline execution context's of all its children.
    // This way you can navigate upstream.
    for (auto executableSuccessor : executableSuccessorPipelines) {
        if (const auto* nextExecutablePipeline = std::get_if<Runtime::Execution::ExecutablePipelinePtr>(&executableSuccessor)) {
            NES_DEBUG("Adding current pipeline: " << executablePipeline->getPipelineId()
                                                  << " as a predecessor to its child pipeline: "
                                                  << (*nextExecutablePipeline)->getPipelineId());
            (*nextExecutablePipeline)->getContext()->addPredecessor(executablePipeline);
        }
        // note: we do not register predecessors for DataSinks.
    }

    executablePipelines.emplace_back(executablePipeline);
    return executablePipeline;
}

SourceDescriptorPtr LowerToExecutableQueryPlanPhase::createSourceDescriptor(SchemaPtr schema, PhysicalSourcePtr physicalSource) {
    auto logicalSourceName = physicalSource->getLogicalSourceName();
    auto physicalSourceName = physicalSource->getPhysicalSourceName();
    auto physicalSourceType = physicalSource->getPhysicalSourceType();
    auto sourceType = physicalSourceType->getSourceType();
    NES_DEBUG("PhysicalSourceConfig: create Actual source descriptor with physical source: " << physicalSource->toString() << " "
                                                                                             << sourceType);

    switch (sourceType) {
        case DEFAULT_SOURCE: {
            auto defaultSourceType = physicalSourceType->as<DefaultSourceType>();
            return DefaultSourceDescriptor::create(
                schema,
                logicalSourceName,
                defaultSourceType->getNumberOfBuffersToProduce()->getValue(),
                std::chrono::milliseconds(defaultSourceType->getSourceGatheringInterval()->getValue()).count());
        }
#ifdef ENABLE_MQTT_BUILD
        case MQTT_SOURCE: {
            auto mqttSourceType = physicalSourceType->as<MQTTSourceType>();
            return MQTTSourceDescriptor::create(schema, mqttSourceType);
        }
#endif
        case CSV_SOURCE: {
            auto csvSourceType = physicalSourceType->as<CSVSourceType>();
            return CsvSourceDescriptor::create(schema, csvSourceType, logicalSourceName, physicalSourceName);
        }
        case SENSE_SOURCE: {
            auto senseSourceType = physicalSourceType->as<SenseSourceType>();
            return SenseSourceDescriptor::create(schema, logicalSourceName, senseSourceType->getUdfs()->getValue());
        }
        case MEMORY_SOURCE: {
            auto memorySourceType = physicalSourceType->as<MemorySourceType>();
            return MemorySourceDescriptor::create(schema,
                                                  memorySourceType->getMemoryArea(),
                                                  memorySourceType->getMemoryAreaSize(),
                                                  memorySourceType->getNumberOfBufferToProduce(),
                                                  memorySourceType->getGatheringValue(),
                                                  memorySourceType->getGatheringMode(),
                                                  memorySourceType->getSourceAffinity(),
                                                  memorySourceType->getTaskQueueId(),
                                                  logicalSourceName,
                                                  physicalSourceName);
        }
        case MONITORING_SOURCE: {
            auto monitoringSourceType = physicalSourceType->as<MonitoringSourceType>();
            return MonitoringSourceDescriptor::create(
                monitoringSourceType->getWaitTime(),
                Monitoring::MetricCollectorType(monitoringSourceType->getMetricCollectorType()));
        }
        case BENCHMARK_SOURCE: {
            auto benchmarkSourceType = physicalSourceType->as<BenchmarkSourceType>();
            return BenchmarkSourceDescriptor::create(schema,
                                                     benchmarkSourceType->getMemoryArea(),
                                                     benchmarkSourceType->getMemoryAreaSize(),
                                                     benchmarkSourceType->getNumberOfBuffersToProduce(),
                                                     benchmarkSourceType->getGatheringValue(),
                                                     benchmarkSourceType->getGatheringMode(),
                                                     benchmarkSourceType->getSourceMode(),
                                                     benchmarkSourceType->getSourceAffinity(),
                                                     benchmarkSourceType->getTaskQueueId(),
                                                     logicalSourceName,
                                                     physicalSourceName);
        }
        case STATIC_DATA_SOURCE: {
            auto staticDataSourceType = physicalSourceType->as<NES::Experimental::StaticDataSourceType>();
            return NES::Experimental::StaticDataSourceDescriptor::create(schema,
                                                                         staticDataSourceType->getPathTableFile(),
                                                                         staticDataSourceType->getLateStart());
        }
        case LAMBDA_SOURCE: {
            auto lambdaSourceType = physicalSourceType->as<LambdaSourceType>();
            return LambdaSourceDescriptor::create(schema,
                                                  lambdaSourceType->getGenerationFunction(),
                                                  lambdaSourceType->getNumBuffersToProduce(),
                                                  lambdaSourceType->getGatheringValue(),
                                                  lambdaSourceType->getGatheringMode(),
                                                  lambdaSourceType->getSourceAffinity(),
                                                  lambdaSourceType->getTaskQueueId(),
                                                  logicalSourceName,
                                                  physicalSourceName);
        }
        case MATERIALIZEDVIEW_SOURCE: {
            auto materializeView =
                physicalSourceType->as<Configurations::Experimental::MaterializedView::MaterializedViewSourceType>();
            return NES::Experimental::MaterializedView::MaterializedViewSourceDescriptor::create(schema,
                                                                                                 materializeView->getId());
        }
        case TCP_SOURCE: {
            auto tcpSourceType = physicalSourceType->as<TCPSourceType>();
            return TCPSourceDescriptor::create(schema, tcpSourceType, logicalSourceName, physicalSourceName);
        }
        case KAFKA_SOURCE: {
            auto kafkaSourceType = physicalSourceType->as<KafkaSourceType>();
            return KafkaSourceDescriptor::create(schema,
                                                 kafkaSourceType->getBrokers()->getValue(),
                                                 logicalSourceName,
                                                 kafkaSourceType->getTopic()->getValue(),
                                                 kafkaSourceType->getGroupId()->getValue(),
                                                 kafkaSourceType->getAutoCommit()->getValue(),
                                                 kafkaSourceType->getConnectionTimeout()->getValue(),
                                                 kafkaSourceType->getOffsetMode()->getValue(),
                                                 kafkaSourceType->getNumberOfBuffersToProduce()->getValue(),
                                                 kafkaSourceType->getBatchSize()->getValue());
        }
        default:
            throw QueryCompilationException("PhysicalSourceConfig:: source type " + physicalSourceType->getSourceTypeAsString()
                                            + " not supported");
    }
}

}// namespace NES::QueryCompilation
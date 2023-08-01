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

#include <Operators/LogicalOperators/Sources/BenchmarkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LambdaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/StaticDataSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>

#include <Monitoring/Util/MetricUtils.hpp>
#include <Network/NetworkManager.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>

#include <Runtime/NodeEngine.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
#if defined(__linux__)
#include <Runtime/HardwareManager.hpp>
#include <numa.h>
#endif
#endif
namespace NES {

DataSourcePtr
ConvertLogicalToPhysicalSource::createDataSource(OperatorId operatorId,
                                                 OriginId originId,
                                                 const SourceDescriptorPtr& sourceDescriptor,
                                                 const Runtime::NodeEnginePtr& nodeEngine,
                                                 size_t numSourceLocalBuffers,
                                                 const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& successors) {
    NES_ASSERT(nodeEngine, "invalid engine");
    auto numaNodeIndex = 0u;
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
    if (sourceDescriptor->instanceOf<BenchmarkSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto benchmarkSourceDescriptor = sourceDescriptor->as<BenchmarkSourceDescriptor>();
        auto sourceAffinity = benchmarkSourceDescriptor->getSourceAffinity();
        if (sourceAffinity != std::numeric_limits<uint64_t>::max()) {
            auto nodeOfCpu = numa_node_of_cpu(sourceAffinity);
            numaNodeIndex = nodeOfCpu;
            NES_ASSERT2_FMT(0 <= numaNodeIndex && numaNodeIndex < nodeEngine->getHardwareManager()->getNumberOfNumaRegions(),
                            "invalid numa settings");
        }
    }
#endif
    auto bufferManager = nodeEngine->getBufferManager(numaNodeIndex);
    auto queryManager = nodeEngine->getQueryManager();
    auto networkManager = nodeEngine->getNetworkManager();

    if (sourceDescriptor->instanceOf<ZmqSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating ZMQ source");
        const ZmqSourceDescriptorPtr zmqSourceDescriptor = sourceDescriptor->as<ZmqSourceDescriptor>();
        return createZmqSource(zmqSourceDescriptor->getSchema(),
                               bufferManager,
                               queryManager,
                               zmqSourceDescriptor->getHost(),
                               zmqSourceDescriptor->getPort(),
                               operatorId,
                               originId,
                               numSourceLocalBuffers,
                               successors);
    }
    if (sourceDescriptor->instanceOf<DefaultSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Default source");
        const DefaultSourceDescriptorPtr defaultSourceDescriptor = sourceDescriptor->as<DefaultSourceDescriptor>();
        return createDefaultDataSourceWithSchemaForVarBuffers(defaultSourceDescriptor->getSchema(),
                                                              bufferManager,
                                                              queryManager,
                                                              defaultSourceDescriptor->getNumbersOfBufferToProduce(),
                                                              defaultSourceDescriptor->getSourceGatheringIntervalCount(),
                                                              operatorId,
                                                              originId,
                                                              numSourceLocalBuffers,
                                                              successors);
    } else if (sourceDescriptor->instanceOf<BinarySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Binary File source");
        const BinarySourceDescriptorPtr binarySourceDescriptor = sourceDescriptor->as<BinarySourceDescriptor>();
        return createBinaryFileSource(binarySourceDescriptor->getSchema(),
                                      bufferManager,
                                      queryManager,
                                      binarySourceDescriptor->getFilePath(),
                                      operatorId,
                                      originId,
                                      numSourceLocalBuffers,
                                      successors);
    } else if (sourceDescriptor->instanceOf<CsvSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating CSV file source");
        const CsvSourceDescriptorPtr csvSourceDescriptor = sourceDescriptor->as<CsvSourceDescriptor>();
        return createCSVFileSource(csvSourceDescriptor->getSchema(),
                                   bufferManager,
                                   queryManager,
                                   csvSourceDescriptor->getSourceConfig(),
                                   operatorId,
                                   originId,
                                   numSourceLocalBuffers,
                                   successors);
#ifdef ENABLE_KAFKA_BUILD
    } else if (sourceDescriptor->instanceOf<KafkaSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating Kafka source");
        const KafkaSourceDescriptorPtr kafkaSourceDescriptor = sourceDescriptor->as<KafkaSourceDescriptor>();
        return createKafkaSource(kafkaSourceDescriptor->getSchema(),
                                 bufferManager,
                                 queryManager,
                                 kafkaSourceDescriptor->getNumberOfToProcessBuffers(),
                                 kafkaSourceDescriptor->getBrokers(),
                                 kafkaSourceDescriptor->getTopic(),
                                 kafkaSourceDescriptor->getGroupId(),
                                 kafkaSourceDescriptor->isAutoCommit(),
                                 kafkaSourceDescriptor->getKafkaConnectTimeout(),
                                 kafkaSourceDescriptor->getOffsetMode(),
                                 operatorId,
                                 originId,
                                 numSourceLocalBuffers,
                                 kafkaSourceDescriptor->getBatchSize(),
                                 successors);
#endif
#ifdef ENABLE_MQTT_BUILD
    } else if (sourceDescriptor->instanceOf<MQTTSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating OPC source");
        const MQTTSourceDescriptorPtr mqttSourceDescriptor = sourceDescriptor->as<MQTTSourceDescriptor>();
        return createMQTTSource(mqttSourceDescriptor->getSchema(),
                                bufferManager,
                                queryManager,
                                mqttSourceDescriptor->getSourceConfigPtr(),
                                operatorId,
                                originId,
                                numSourceLocalBuffers,
                                successors);
#endif
#ifdef ENABLE_OPC_BUILD
    } else if (sourceDescriptor->instanceOf<OPCSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating OPC source");
        const OPCSourceDescriptorPtr opcSourceDescriptor = sourceDescriptor->as<OPCSourceDescriptor>();
        return createOPCSource(opcSourceDescriptor->getSchema(),
                               bufferManager,
                               queryManager,
                               opcSourceDescriptor->getUrl(),
                               opcSourceDescriptor->getNodeId(),
                               opcSourceDescriptor->getUser(),
                               opcSourceDescriptor->getPassword(),
                               operatorId,
                               numSourceLocalBuffers,
                               successors);
#endif
    } else if (sourceDescriptor->instanceOf<SenseSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating sense source");
        const SenseSourceDescriptorPtr senseSourceDescriptor = sourceDescriptor->as<SenseSourceDescriptor>();
        return createSenseSource(senseSourceDescriptor->getSchema(),
                                 bufferManager,
                                 queryManager,
                                 senseSourceDescriptor->getUdfs(),
                                 operatorId,
                                 originId,
                                 numSourceLocalBuffers,
                                 successors);
    } else if (sourceDescriptor->instanceOf<Network::NetworkSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating network source");
        const Network::networkSourceDescriptorPtr networkSourceDescriptor =
            sourceDescriptor->as<Network::NetworkSourceDescriptor>();
        return createNetworkSource(networkSourceDescriptor->getSchema(),
                                   bufferManager,
                                   queryManager,
                                   networkManager,
                                   networkSourceDescriptor->getNesPartition(),
                                   networkSourceDescriptor->getNodeLocation(),
                                   numSourceLocalBuffers,
                                   networkSourceDescriptor->getWaitTime(),
                                   networkSourceDescriptor->getRetryTimes(),
                                   successors);
    } else if (sourceDescriptor->instanceOf<MemorySourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto memorySourceDescriptor = sourceDescriptor->as<MemorySourceDescriptor>();
        return createMemorySource(memorySourceDescriptor->getSchema(),
                                  bufferManager,
                                  queryManager,
                                  memorySourceDescriptor->getMemoryArea(),
                                  memorySourceDescriptor->getMemoryAreaSize(),
                                  memorySourceDescriptor->getNumBuffersToProcess(),
                                  memorySourceDescriptor->getGatheringValue(),
                                  operatorId,
                                  originId,
                                  numSourceLocalBuffers,
                                  memorySourceDescriptor->getGatheringMode(),
                                  memorySourceDescriptor->getSourceAffinity(),
                                  memorySourceDescriptor->getTaskQueueId(),
                                  successors);
    } else if (sourceDescriptor->instanceOf<MonitoringSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating monitoring source");
        auto monitoringSourceDescriptor = sourceDescriptor->as<MonitoringSourceDescriptor>();
        auto metricCollector =
            Monitoring::MetricUtils::createCollectorFromCollectorType(monitoringSourceDescriptor->getMetricCollectorType());
        metricCollector->setNodeId(nodeEngine->getNodeId());
        return createMonitoringSource(metricCollector,
                                      monitoringSourceDescriptor->getWaitTime(),
                                      bufferManager,
                                      queryManager,
                                      operatorId,
                                      originId,
                                      numSourceLocalBuffers,
                                      successors);
    } else if (sourceDescriptor->instanceOf<NES::Experimental::StaticDataSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating static data source");
        auto staticDataSourceDescriptor = sourceDescriptor->as<NES::Experimental::StaticDataSourceDescriptor>();
        return NES::Experimental::createStaticDataSource(staticDataSourceDescriptor->getSchema(),
                                                         staticDataSourceDescriptor->getPathTableFile(),
                                                         staticDataSourceDescriptor->getLateStart(),
                                                         bufferManager,
                                                         queryManager,
                                                         operatorId,
                                                         originId,
                                                         numSourceLocalBuffers,
                                                         successors);
    } else if (sourceDescriptor->instanceOf<BenchmarkSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating memory source");
        auto benchmarkSourceDescriptor = sourceDescriptor->as<BenchmarkSourceDescriptor>();
        return createBenchmarkSource(benchmarkSourceDescriptor->getSchema(),
                                     bufferManager,
                                     queryManager,
                                     benchmarkSourceDescriptor->getMemoryArea(),
                                     benchmarkSourceDescriptor->getMemoryAreaSize(),
                                     benchmarkSourceDescriptor->getNumBuffersToProcess(),
                                     benchmarkSourceDescriptor->getGatheringValue(),
                                     operatorId,
                                     originId,
                                     numSourceLocalBuffers,
                                     benchmarkSourceDescriptor->getGatheringMode(),
                                     benchmarkSourceDescriptor->getSourceMode(),
                                     benchmarkSourceDescriptor->getSourceAffinity(),
                                     benchmarkSourceDescriptor->getTaskQueueId(),
                                     successors);
    } else if (sourceDescriptor->instanceOf<LambdaSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating lambda source");
        auto lambdaSourceDescriptor = sourceDescriptor->as<LambdaSourceDescriptor>();
        return createLambdaSource(lambdaSourceDescriptor->getSchema(),
                                  bufferManager,
                                  queryManager,
                                  lambdaSourceDescriptor->getNumBuffersToProcess(),
                                  lambdaSourceDescriptor->getGatheringValue(),
                                  std::move(lambdaSourceDescriptor->getGeneratorFunction()),
                                  operatorId,
                                  originId,
                                  numSourceLocalBuffers,
                                  lambdaSourceDescriptor->getGatheringMode(),
                                  lambdaSourceDescriptor->getSourceAffinity(),
                                  lambdaSourceDescriptor->getTaskQueueId(),
                                  successors);
    } else if (sourceDescriptor->instanceOf<NES::Experimental::MaterializedView::MaterializedViewSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating materialized view source");
        auto materializedViewSourceDescriptor =
            sourceDescriptor->as<NES::Experimental::MaterializedView::MaterializedViewSourceDescriptor>();
        auto viewId = materializedViewSourceDescriptor->getViewId();
        NES::Experimental::MaterializedView::MaterializedViewPtr view = nullptr;
        if (nodeEngine->getMaterializedViewManager()->containsView(viewId)) {
            view = nodeEngine->getMaterializedViewManager()->getView(viewId);
        } else {
            view = nodeEngine->getMaterializedViewManager()->createView(NES::Experimental::MaterializedView::ViewType::TUPLE_VIEW,
                                                                        viewId);
        }
        return NES::Experimental::MaterializedView::createMaterializedViewSource(materializedViewSourceDescriptor->getSchema(),
                                                                                 bufferManager,
                                                                                 queryManager,
                                                                                 operatorId,
                                                                                 originId,
                                                                                 numSourceLocalBuffers,
                                                                                 successors,
                                                                                 std::move(view));
    } else if (sourceDescriptor->instanceOf<TCPSourceDescriptor>()) {
        NES_INFO("ConvertLogicalToPhysicalSource: Creating TCP source");
        auto tcpSourceDescriptor = sourceDescriptor->as<TCPSourceDescriptor>();
        return createTCPSource(tcpSourceDescriptor->getSchema(),
                               bufferManager,
                               queryManager,
                               tcpSourceDescriptor->getSourceConfig(),
                               operatorId,
                               originId,
                               numSourceLocalBuffers,
                               successors);
    } else {
        NES_ERROR("ConvertLogicalToPhysicalSource: Unknown Source Descriptor Type " << sourceDescriptor->getSchema()->toString());
        throw std::invalid_argument("Unknown Source Descriptor Type");
    }
}

}// namespace NES

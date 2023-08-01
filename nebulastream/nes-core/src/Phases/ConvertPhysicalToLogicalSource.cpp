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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MQTTSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MaterializedViewSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MemorySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/MonitoringSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/OPCSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Phases/ConvertPhysicalToLogicalSource.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/KafkaSource.hpp>
#include <Sources/MQTTSource.hpp>
#include <Sources/MaterializedViewSource.hpp>
#include <Sources/MemorySource.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Sources/OPCSource.hpp>
#include <Sources/SenseSource.hpp>
#include <Sources/TCPSource.hpp>
#include <Sources/ZmqSource.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SourceDescriptorPtr ConvertPhysicalToLogicalSource::createSourceDescriptor(const DataSourcePtr& dataSource) {
    SourceType srcType = dataSource->getType();
    switch (srcType) {
        case ZMQ_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating ZMQ source");
            const ZmqSourcePtr zmqSourcePtr = std::dynamic_pointer_cast<ZmqSource>(dataSource);
            SourceDescriptorPtr zmqSourceDescriptor =
                ZmqSourceDescriptor::create(dataSource->getSchema(), zmqSourcePtr->getHost(), zmqSourcePtr->getPort());
            return zmqSourceDescriptor;
        }
        case DEFAULT_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Default source");
            const DefaultSourcePtr defaultSourcePtr = std::dynamic_pointer_cast<DefaultSource>(dataSource);
            const SourceDescriptorPtr defaultSourceDescriptor =
                DefaultSourceDescriptor::create(defaultSourcePtr->getSchema(),
                                                defaultSourcePtr->getNumBuffersToProcess(),
                                                defaultSourcePtr->getGatheringIntervalCount());
            return defaultSourceDescriptor;
        }
        case BINARY_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Binary File source");
            const BinarySourcePtr binarySourcePtr = std::dynamic_pointer_cast<BinarySource>(dataSource);
            const SourceDescriptorPtr binarySourceDescriptor =
                BinarySourceDescriptor::create(binarySourcePtr->getSchema(), binarySourcePtr->getFilePath());
            return binarySourceDescriptor;
        }
        case CSV_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating CSV File source");
            const CSVSourcePtr csvSourcePtr = std::dynamic_pointer_cast<CSVSource>(dataSource);
            const SourceDescriptorPtr csvSourceDescriptor =
                CsvSourceDescriptor::create(csvSourcePtr->getSchema(), csvSourcePtr->getSourceConfig());
            return csvSourceDescriptor;
        }
#ifdef ENABLE_KAFKA_BUILD
        case KAFKA_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating Kafka source");
            const KafkaSourcePtr kafkaSourcePtr = std::dynamic_pointer_cast<KafkaSource>(dataSource);
            const SourceDescriptorPtr kafkaSourceDescriptor =
                KafkaSourceDescriptor::create(kafkaSourcePtr->getSchema(),
                                              kafkaSourcePtr->getBrokers(),
                                              kafkaSourcePtr->getTopic(),
                                              kafkaSourcePtr->getGroupId(),
                                              kafkaSourcePtr->isAutoCommit(),
                                              kafkaSourcePtr->getKafkaConsumerTimeout().count(),
                                              kafkaSourcePtr->getOffsetMode(),
                                              kafkaSourcePtr->getNumBuffersToProcess(),
                                              kafkaSourcePtr->getBatchSize());
            return kafkaSourceDescriptor;
        }
#endif
#ifdef ENABLE_MQTT_BUILD
        case MQTT_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating MQTT source");
            const MQTTSourcePtr mqttSourcePtr = std::dynamic_pointer_cast<MQTTSource>(dataSource);
            const SourceDescriptorPtr mqttSourceDescriptor =
                MQTTSourceDescriptor::create(mqttSourcePtr->getSchema(), mqttSourcePtr->getSourceConfigPtr());
            return mqttSourceDescriptor;
        }
#endif
#ifdef ENABLE_OPC_BUILD
        case OPC_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating OPC source");
            const OPCSourcePtr opcSourcePtr = std::dynamic_pointer_cast<OPCSource>(dataSource);
            const SourceDescriptorPtr opcSourceDescriptor = OPCSourceDescriptor::create(opcSourcePtr->getSchema(),
                                                                                        opcSourcePtr->getUrl(),
                                                                                        opcSourcePtr->getNodeId(),
                                                                                        opcSourcePtr->getUser(),
                                                                                        opcSourcePtr->getPassword());
            return opcSourceDescriptor;
        }
#endif
        case SENSE_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating sense source");
            const SenseSourcePtr senseSourcePtr = std::dynamic_pointer_cast<SenseSource>(dataSource);
            const SourceDescriptorPtr senseSourceDescriptor =
                SenseSourceDescriptor::create(senseSourcePtr->getSchema(), senseSourcePtr->getUdfs());
            return senseSourceDescriptor;
        }
        case MATERIALIZEDVIEW_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating materialized view source");
            const Experimental::MaterializedView::MaterializedViewSourcePtr materializedViewSourcePtr =
                std::dynamic_pointer_cast<Experimental::MaterializedView::MaterializedViewSource>(dataSource);
            const SourceDescriptorPtr materializedViewSourceDescriptor =
                Experimental::MaterializedView::MaterializedViewSourceDescriptor::create(materializedViewSourcePtr->getSchema(),
                                                                                         materializedViewSourcePtr->getViewId());
            return materializedViewSourceDescriptor;
        }
        case MEMORY_SOURCE: {
            NES_ASSERT(false, "not supported because MemorySouce must be used only for local development or testing");
        }
        case MONITORING_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating monitoring source");
            const MonitoringSourcePtr monitoringSource = std::dynamic_pointer_cast<MonitoringSource>(dataSource);
            const SourceDescriptorPtr monitoringSourceDescriptor =
                MonitoringSourceDescriptor::create(monitoringSource->getWaitTime(), monitoringSource->getCollectorType());
            return monitoringSourceDescriptor;
        }
        case TCP_SOURCE: {
            NES_INFO("ConvertPhysicalToLogicalSource: Creating TCP source");
            const TCPSourcePtr tcpSource = std::dynamic_pointer_cast<TCPSource>(dataSource);
            const SourceDescriptorPtr tcpSourceDescriptor =
                TCPSourceDescriptor::create(tcpSource->getSchema(), tcpSource->getSourceConfig());
            return tcpSourceDescriptor;
        }
        default: {
            NES_ERROR("ConvertPhysicalToLogicalSource: Unknown Data Source Type " << srcType);
            throw std::invalid_argument("Unknown Source Descriptor Type ");
        }
    }
}

}// namespace NES

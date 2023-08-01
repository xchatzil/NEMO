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

#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/MaterializedViewSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/OPCSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Phases/ConvertPhysicalToLogicalSink.hpp>
#include <Sinks/Mediums/FileSink.hpp>
#include <Sinks/Mediums/KafkaSink.hpp>
#include <Sinks/Mediums/MQTTSink.hpp>
#include <Sinks/Mediums/MaterializedViewSink.hpp>
#include <Sinks/Mediums/OPCSink.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SinkDescriptorPtr ConvertPhysicalToLogicalSink::createSinkDescriptor(const DataSinkPtr& dataSink) {
    std::string sinkType = dataSink->toString();

    if (sinkType == "PRINT_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating print sink");
        return PrintSinkDescriptor::create();
    }
    if (sinkType == "ZMQ_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating ZMQ sink");
        ZmqSinkPtr zmqSink = std::dynamic_pointer_cast<ZmqSink>(dataSink);
        return ZmqSinkDescriptor::create(zmqSink->getHost(), zmqSink->getPort());
    }
#ifdef ENABLE_KAFKA_BUILD
    else if (sinkType == "KAFKA_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating Kafka sink");
        KafkaSinkPtr kafkaSink = std::dynamic_pointer_cast<KafkaSink>(dataSink);
        return KafkaSinkDescriptor::create(kafkaSink->getSinkFormat(),
                                           kafkaSink->getTopic(),
                                           kafkaSink->getBrokers(),
                                           kafkaSink->getKafkaProducerTimeout());
    }
#endif
#ifdef ENABLE_OPC_BUILD
    else if (sinkType == "OPC_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating OPC sink");
        OPCSinkPtr opcSink = std::dynamic_pointer_cast<OPCSink>(dataSink);
        return OPCSinkDescriptor::create(opcSink->getUrl(), opcSink->getNodeId(), opcSink->getUser(), opcSink->getPassword());
    }
#endif
#ifdef ENABLE_MQTT_BUILD
    else if (sinkType == "MQTT_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating MQTT sink");
        MQTTSinkPtr mqttSink = std::dynamic_pointer_cast<MQTTSink>(dataSink);
        return MQTTSinkDescriptor::create(mqttSink->getAddress(),
                                          mqttSink->getAddress(),
                                          mqttSink->getTopic(),
                                          mqttSink->getMaxBufferedMSGs(),
                                          mqttSink->getTimeUnit(),
                                          mqttSink->getMsgDelay(),
                                          mqttSink->getQualityOfService(),
                                          mqttSink->getAsynchronousClient(),
                                          mqttSink->getUser());
    }
#endif
    else if (sinkType == "FILE_SINK") {
        FileSinkPtr fileSink = std::dynamic_pointer_cast<FileSink>(dataSink);
        NES_INFO("ConvertPhysicalToLogicalSink: Creating File sink with outputMode " << fileSink->getAppendAsBool() << " format "
                                                                                     << fileSink->getSinkFormat());
        return FileSinkDescriptor::create(fileSink->getFilePath(), fileSink->getSinkFormat(), fileSink->getAppendAsString());
    } else if (sinkType == "MATERIALIZED_VIEW_SINK") {
        NES_INFO("ConvertPhysicalToLogicalSink: Creating materialized view sink");
        Experimental::MaterializedView::MaterializedViewSinkPtr materializedViewSink =
            std::dynamic_pointer_cast<Experimental::MaterializedView::MaterializedViewSink>(dataSink);
        return Experimental::MaterializedView::MaterializedViewSinkDescriptor::create(materializedViewSink->getViewId());
    } else {
        NES_ERROR("ConvertPhysicalToLogicalSink: Unknown Data Sink Type");
        throw std::invalid_argument("Unknown SinkMedium Type");
    }
}
}// namespace NES

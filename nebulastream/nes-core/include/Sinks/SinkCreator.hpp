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

#ifndef NES_CORE_INCLUDE_SINKS_SINKCREATOR_HPP_
#define NES_CORE_INCLUDE_SINKS_SINKCREATOR_HPP_
#include <Network/NetworkSink.hpp>
#include <Operators/LogicalOperators/Sinks/MQTTSinkDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#ifdef ENABLE_OPC_BUILD
#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>
#endif
#ifdef ENABLE_MQTT_BUILD
#include <Sinks/Mediums/MQTTSink.hpp>
#endif

namespace NES {
/**
 * @brief create test sink
 * @Note this method is currently not implemented
 */
DataSinkPtr createTestSink();

/**
 * @brief create a csv test sink without a schema and append to existing file
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */

DataSinkPtr createCSVFileSink(const SchemaPtr& schema,
                              QueryId queryId,
                              QuerySubPlanId querySubPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              uint32_t activeProducers,
                              const std::string& filePath,
                              bool append,
                              FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                              uint64_t numberOfOrigins = 1);

/**
 * @brief create a binary test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createTextFileSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t numOfProducers,
                               const std::string& filePath,
                               bool append,
                               FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a binary test sink with a schema into the nes
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createBinaryNESFileSink(const SchemaPtr& schema,
                                    QueryId queryId,
                                    QuerySubPlanId querySubPlanId,
                                    const Runtime::NodeEnginePtr& nodeEngine,
                                    uint32_t numOfProducers,
                                    const std::string& filePath,
                                    bool append,
                                    FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                    uint64_t numberOfOrigins = 1);

/**
 * @brief create a JSON test sink with a schema int
 * @param schema of sink
 * @param bufferManager
 * @param path to file
 * @param bool indicating if data is appended (true) or overwritten (false)
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createJSONFileSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t numOfProducers,
                               const std::string& filePath,
                               bool append,
                               FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a ZMQ test sink with a schema and Text format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @param internal refers to the usage of this zmq sink as a fwd operator such that we dont have to send the schema, only the data
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createTextZmqSink(const SchemaPtr& schema,
                              QueryId queryId,
                              QuerySubPlanId querySubPlanId,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              uint32_t numOfProducers,
                              const std::string& host,
                              uint16_t port,
                              FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                              uint64_t numberOfOrigins = 1);
#ifdef ENABLE_OPC_BUILD
/**
 * @brief create a OPC test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param url to OPC server as string
 * @param nodeId to save data in
 * @param user name as string to log in to the OPC server
 * @param password as string to log in to the OPC server
 * @return a data sink pointer
 */
DataSinkPtr createOPCSink(SchemaPtr schema,
                          QuerySubPlanId querySubPlanId,
                          Runtime::NodeEnginePtr nodeEngine,
                          std::string url,
                          UA_NodeId nodeId,
                          std::string user,
                          std::string password);
#endif
/**
 * @brief create a ZMQ test sink with a schema and CSV format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCSVZmqSink(const SchemaPtr& schema,
                             QueryId queryId,
                             QuerySubPlanId querySubPlanId,
                             const Runtime::NodeEnginePtr& nodeEngine,
                             uint32_t numOfProducers,
                             const std::string& host,
                             uint16_t port,
                             FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                             uint64_t numberOfOrigins = 1);

/**
 * @brief create a ZMQ test sink with a schema and NES_FORMAT format output
 * @param schema of sink
 * @param bufferManager
 * @param hostname as sting
 * @param port at uint16
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createBinaryZmqSink(const SchemaPtr& schema,
                                QueryId queryId,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                const std::string& host,
                                uint16_t port,
                                bool internal,
                                FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param bufferManager
 * @param output stream
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createTextPrintSink(const SchemaPtr& schema,
                                QueryId queryId,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                std::ostream& out,
                                FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                uint64_t numberOfOrigins = 1);

/**
 * @brief create a print that does not output something
 * @return a data sink pointer
 */
DataSinkPtr createNullOutputSink(QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 const Runtime::NodeEnginePtr& nodeEngine,
                                 uint32_t activeProducers,
                                 FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                 uint64_t numberOfOrigins = 1);

/**
 * @brief create a print test sink with a schema
 * @param schema of sink
 * @param parentPlan id of the parent qep
 * @param bufferManager
 * @param output stream
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createCSVPrintSink(const SchemaPtr& schema,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               const Runtime::NodeEnginePtr& nodeEngine,
                               uint32_t activeProducers,
                               std::ostream& out,
                               FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                               uint64_t numberOfOrigins = 1);

/**
 * @brief create a network data sink
 * @param schema
 * @param networkManager
 * @param nodeLocation
 * @param nesPartition
 * @param waitTime
 * @param retryTimes
 * @param faultToleranceType: fault tolerance type of a query
 * @return a data sink pointer
 */
DataSinkPtr createNetworkSink(const SchemaPtr& schema,
                              uint64_t uniqueNetworkSinkDescriptorId,
                              QueryId queryId,
                              QuerySubPlanId querySubPlanId,
                              const Network::NodeLocation& nodeLocation,
                              Network::NesPartition nesPartition,
                              const Runtime::NodeEnginePtr& nodeEngine,
                              size_t numOfProducers,
                              std::chrono::milliseconds waitTime,
                              FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                              uint64_t numberOfOrigins = 1,
                              uint8_t retryTimes = 5);

/**
 * @brief create a monitoring data sink
 * @param metricStore
 * @param nodeEngine
 * @param numOfProducers
 * @param queryId
 * @param querySubPlanId
 * @param faultToleranceType
 * @param numberOfOrigins
 * @return a data sink pointer
 */
DataSinkPtr createMonitoringSink(Monitoring::MetricStorePtr metricStore,
                                 Monitoring::MetricCollectorType collectorType,
                                 const SchemaPtr& schema,
                                 Runtime::NodeEnginePtr nodeEngine,
                                 uint32_t numOfProducers,
                                 QueryId queryId,
                                 QuerySubPlanId querySubPlanId,
                                 FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                 uint64_t numberOfOrigins = 1);

namespace Experimental::MaterializedView {

/**
 * @brief create materialized view sink
 * @param schema
 * @param nodeEngine
 * @param parentPlanId
 * @param viewId
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createMaterializedViewSink(SchemaPtr schema,
                                       const Runtime::NodeEnginePtr& nodeEngine,
                                       uint32_t numOfProducers,
                                       QueryId queryId,
                                       QuerySubPlanId parentPlanId,
                                       uint64_t viewId,
                                       FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                       uint64_t numberOfOrigins = 1);

}// namespace Experimental::MaterializedView
#ifdef ENABLE_KAFKA_BUILD
/**
 * @brief create kafka sink
 * @param schema: schema of the data
 * @param queryId of the query that writes to the sink
 * @param querySubPlanId of the query that writes to the sink
 * @param nodeEngine
 * @param activeProducers how many different queries write to this sink which is needed for sink shutdown
 * @param brokers: broker list
 * @param topic: kafka topic to write to
 * @param kafkaProducerTimeout: kafka producer timeout
 * @param faultToleranceType
 * @param numberOfOrigins
 * @return a data sink pointer
 */
DataSinkPtr createTextKafkaSink(SchemaPtr schema,
                                QueryId queryId,
                                QuerySubPlanId querySubPlanId,
                                const Runtime::NodeEnginePtr& nodeEngine,
                                uint32_t activeProducers,
                                const std::string& brokers,
                                const std::string& topic,
                                uint64_t kafkaProducerTimeout,
                                FaultToleranceType::Value faultToleranceType,
                                uint64_t numberOfOrigins);
#endif
#ifdef ENABLE_MQTT_BUILD
/**
 * @brief create MQTT sink
 * @param schema: schema of the data
 * @param parentPlan: the Id of the parent plan
 * @param nodeEngine: a node engine pointer, e.g. for access to the buffer manager
 * @param address: address of a MQTT broker
 * @param clientId: client ID for MQTT client. If non is given, the operatorID is used automatically (see 'ConvertLogicalToPhysicalSink.cpp).
 * @param topic: broker topic/path to which the MQTT messages are published
 * @param user: used to identify client at broker
 * @param maxBufferedMSGs: maximal number for how many messages can be buffered at client
 * @param timeUnit: unit used to interpret the msgDelay value (seconds vs milliseconds vs nanoseconds)
 * @param msgDelay: how long to wait before sending the next message from client
 * @param qualityOfService: either 'at most once' or 'at least once'. QOS > 0 required for a non-clean (persistent) session.
 * @param asynchronousClient: 1 if client should be asynchronous, else 0
 * @param faultToleranceType: fault tolerance type of a query
 * @param numberOfOrigins: number of origins of a given query
 * @return a data sink pointer
 */
DataSinkPtr createMQTTSink(const SchemaPtr& schema,
                           QueryId queryId,
                           QuerySubPlanId querySubPlanId,
                           const Runtime::NodeEnginePtr& nodeEngine,
                           uint32_t numOfProducers,
                           std::string const& address,
                           std::string const& clientID,
                           std::string const& topic,
                           std::string const& user,
                           uint64_t maxBufferedMSGs,
                           MQTTSinkDescriptor::TimeUnits timeUnit,
                           uint64_t msgDelay,
                           MQTTSinkDescriptor::ServiceQualities qualityOfService,
                           bool asynchronousClient,
                           FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                           uint64_t numberOfOrigins = 1);
#endif

}// namespace NES
#endif// NES_CORE_INCLUDE_SINKS_SINKCREATOR_HPP_

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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/LocationFactory.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Runtime/QueryExecutionMode.hpp>
#include <Spatial/Index/Location.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <map>
#include <string>

namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

/**
 * @brief object for storing worker configuration
 */
class WorkerConfiguration : public BaseConfiguration {
  public:
    WorkerConfiguration() : BaseConfiguration(){};
    WorkerConfiguration(std::string name, std::string description) : BaseConfiguration(name, description){};

    /**
     * @brief IP of the Worker.
     */
    StringOption localWorkerIp = {LOCAL_WORKER_IP_CONFIG, "127.0.0.1", "Worker IP."};

    /**
     * @brief Port for the RPC server of the Worker.
     * This is used to receive control messages from the coordinator or other workers .
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, 0, "RPC server port of the NES Worker."};

    /**
     * @brief Port of the Data server of this worker.
     * This is used to receive data.
     */
    UIntOption dataPort = {DATA_PORT_CONFIG, 0, "Data port of the NES Worker."};

    /**
     * @brief Server IP of the NES Coordinator to which the NES Worker should connect.
     */
    StringOption coordinatorIp = {COORDINATOR_IP_CONFIG,
                                  "127.0.0.1",
                                  "Server IP of the NES Coordinator to which the NES Worker should connect."};
    /**
     * @brief RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs
     * to be the same as rpcPort in Coordinator.
     */
    UIntOption coordinatorPort = {
        COORDINATOR_PORT_CONFIG,
        4000,
        "RPC server Port of the NES Coordinator to which the NES Worker should connect. Needs to be set and needs "
        "to be the same as rpcPort in Coordinator."};

    /**
     * @brief Parent ID of this node.
     */
    UIntOption parentId = {PARENT_ID_CONFIG, 0, "Parent ID of this node."};

    /**
     * @brief The current log level. Controls the detail of log messages.
     */
    EnumOption<LogLevel> logLevel = {LOG_LEVEL_CONFIG,
                                     LogLevel::LOG_INFO,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    /**
     * @brief Number of Slots define the amount of computing resources that are usable at the coordinator.
     * This enables the restriction of the amount of concurrently deployed queryIdAndCatalogEntryMapping and operators.
     */
    UIntOption numberOfSlots = {NUMBER_OF_SLOTS_CONFIG, UINT16_MAX, "Number of computing slots for the NES Worker."};

    /**
     * @brief Configures the number of worker threads.
     */
    UIntOption numWorkerThreads = {"numWorkerThreads", 1, "Number of worker threads."};

    /**
     * @brief The number of buffers in the global buffer manager.
     * Controls how much memory is consumed by the system.
     */
    UIntOption numberOfBuffersInGlobalBufferManager = {NUMBER_OF_BUFFERS_IN_GLOBAL_BUFFER_MANAGER_CONFIG,
                                                       1024,
                                                       "Number buffers in global buffer pool."};
    /**
     * @brief Indicates how many buffers a single worker thread can allocate.
     */
    UIntOption numberOfBuffersPerWorker = {NUMBER_OF_BUFFERS_PER_WORKER_CONFIG, 128, "Number buffers in task local buffer pool."};

    /**
     * @brief Indicates how many buffers a single data source can allocate.
     * This property controls the backpressure mechanism as a data source that can't allocate new records can't ingest more data.
     */
    UIntOption numberOfBuffersInSourceLocalBufferPool = {NUMBER_OF_BUFFERS_IN_SOURCE_LOCAL_BUFFER_POOL_CONFIG,
                                                         64,
                                                         "Number buffers in source local buffer pool."};
    /**
     * @brief Configures the buffer size of individual TupleBuffers in bytes.
     * This property has to be the same over a whole deployment.
     */
    UIntOption bufferSizeInBytes = {BUFFERS_SIZE_IN_BYTES_CONFIG, 4096, "BufferSizeInBytes."};

    /**
     * @brief Configures the buffer size of individual TupleBuffers in bytes.
     * This property has to be the same over a whole deployment.
     */
    UIntOption monitoringWaitTime = {MONITORING_WAIT_TIME, 1000, "Sampling period of metrics (ms)."};

    /**
     * @brief Indicates a list of cpu cores, which are used to pin data sources to specific cores.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption sourcePinList = {SOURCE_PIN_LIST_CONFIG, "", "comma separated list of where to map the sources"};

    /**
     * @brief Indicates a list of cpu cores, which are used  to pin worker threads to specific cores.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption workerPinList = {WORKER_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker"};

    /**
     * @brief Pins specific worker threads to specific queues.
     * @deprecated this value is deprecated and will be removed.
     */
    StringOption queuePinList = {QUEUE_PIN_LIST_CONFIG, "", "comma separated list of where to map the worker on the queue"};

    /**
     * @brief Enables support for Non-Uniform Memory Access (NUMA) systems.
     */
    BoolOption numaAwareness = {NUMA_AWARENESS_CONFIG, false, "Enable Numa-Aware execution"};

    /**
     * @brief Enables the monitoring stack
     */
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG, false, "Enable monitoring"};

    /**
     * @brief Enables source sharing
     * */
    BoolOption enableSourceSharing = {ENABLE_SOURCE_SHARING_CONFIG, false, "Enable source sharing"};

    /**
     * @brief Enables the statistic output
     */
    BoolOption enableStatisticOuput = {ENABLE_STATISTIC_OUTPUT_CONFIG, false, "Enable statistic output"};

    /**
     * @brief Sets configuration properties for the query compiler.
     */
    QueryCompilerConfiguration queryCompiler = {QUERY_COMPILER_CONFIG, "Configuration for the query compiler"};

    /**
     * @brief Indicates a sequence of physical sources.
     */
    SequenceOption<WrapOption<PhysicalSourcePtr, PhysicalSourceFactory>> physicalSources = {PHYSICAL_SOURCES, "Physical sources"};

    /**
     * @brief location coordinate of the node if any
     */
    WrapOption<NES::Spatial::Index::Experimental::Location, Configurations::Spatial::Index::Experimental::LocationFactory>
        locationCoordinates = {LOCATION_COORDINATES_CONFIG, "the physical location of the worker"};

    /**
     * @brief specify if the worker is running on a mobile device, if it is a node with a known fixed loction, or if it
     * does not have a known location.
     */
    EnumOption<NES::Spatial::Index::Experimental::NodeType> nodeSpatialType = {
        SPATIAL_TYPE_CONFIG,
        NES::Spatial::Index::Experimental::NodeType::NO_LOCATION,
        "specifies if the worker has no known location or if it is a fixed location node or mobile node"};

    /**
     * @brief specifies the path to a yaml file containing a mobility configuration
     */
    Spatial::Mobility::Experimental::WorkerMobilityConfiguration mobilityConfiguration = {
        MOBILITY_CONFIG_CONFIG,
        "the configuration data for the location provider class"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

#ifdef TFDEF
    BoolOption isTfInstalled = {TF_INSTALLED_CONFIG, false, "TF lite installed"};
#endif// TFDEF
    /**
     * @brief Factory function for a worker config
     */
    static std::shared_ptr<WorkerConfiguration> create() { return std::make_shared<WorkerConfiguration>(); }

    /**
     * @brief Configuration numberOfQueues.
     * Set the number of processing queues in the system
     */
    UIntOption numberOfQueues = {NUMBER_OF_QUEUES, 1, "Number of processing queues."};

    /**
     * @brief Configuration numberOfThreadsPerQueue.
     * Set the number of threads per processing queue in the system
     */
    UIntOption numberOfThreadsPerQueue = {NUMBER_OF_THREAD_PER_QUEUE, 0, "Number of threads per processing queue."};

    /**
     * @brief Number of buffers per epoch
     * Set trimming frequency for upstream backup
     */
    UIntOption numberOfBuffersPerEpoch = {NUMBER_OF_BUFFERS_PER_EPOCH, 100, "Number of tuple buffers allowed in one epoch."};

    /**
     * @brief Configuration queryManagerMode
     * The modus in which the query manager is running
     *      - Dynamic: only one queue overall
     *      - Static: use queue per query and a specified number of threads per queue
     */
    EnumOption<Runtime::QueryExecutionMode> queryManagerMode = {
        QUERY_MANAGER_MODE,
        Runtime::QueryExecutionMode::Dynamic,
        "Which mode the query manager is running in. (Dynamic, Static, NumaAware, Invalid)"};

    /**
     * @brief Configuration of waiting time of the worker health check.
     * Set the number of seconds waiting to perform health checks
     */
    UIntOption workerHealthCheckWaitTime = {HEALTH_CHECK_WAIT_TIME, 1, "Number of seconds to wait between health checks"};

    /* Network specific settings */

    IntOption senderHighwatermark = {SENDER_HIGH_WATERMARK,
                                     8,
                                     "Number of tuple buffers allowed in one network channel before blocking transfer."};

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&localWorkerIp,
                &coordinatorIp,
                &rpcPort,
                &dataPort,
                &coordinatorPort,
                &numberOfSlots,
                &numWorkerThreads,
                &numberOfBuffersInGlobalBufferManager,
                &numberOfBuffersPerWorker,
                &numberOfBuffersInSourceLocalBufferPool,
                &bufferSizeInBytes,
                &monitoringWaitTime,
                &parentId,
                &logLevel,
                &sourcePinList,
                &workerPinList,
                &queuePinList,
                &numaAwareness,
                &enableMonitoring,
                &queryCompiler,
                &physicalSources,
                &locationCoordinates,
                &nodeSpatialType,
                &mobilityConfiguration,
                &numberOfQueues,
                &numberOfThreadsPerQueue,
                &numberOfBuffersPerEpoch,
                &queryManagerMode,
                &enableSourceSharing,
                &workerHealthCheckWaitTime,
                &configPath,
#ifdef TFDEF
                &isTfInstalled
#endif
        };
    }
};
}// namespace Configurations
}// namespace NES

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_WORKERCONFIGURATION_HPP_

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

#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_

#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Coordinator/LogicalSourceFactory.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <REST/ServerTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace NES {

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

namespace Configurations {

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

/**
 * @brief Configuration options for the Coordinator.
 */
class CoordinatorConfiguration : public BaseConfiguration {
  public:
    /**
     * @brief IP of the REST server.
     */
    StringOption restIp = {REST_IP_CONFIG, "127.0.0.1", "NES ip of the REST server."};

    /**
     * @brief Port of the REST server.
     */
    UIntOption restPort = {REST_PORT_CONFIG, 8081, "Port exposed for rest endpoints"};

    /**
     * @brief IP of the Coordinator.
     */
    StringOption coordinatorIp = {COORDINATOR_IP_CONFIG, "127.0.0.1", "RPC IP address of NES Coordinator."};

    /**
     * @brief Port for the RPC server of the Coordinator.
     * This is used to receive control messages.
     */
    UIntOption rpcPort = {RPC_PORT_CONFIG, 4000, "RPC server port of the Coordinator"};

    /**
     * @brief Port of the Data server of the Coordinator.
     * This is used to receive data at the coordinator.
     */
    UIntOption dataPort = {DATA_PORT_CONFIG, 0, "Data server port of the Coordinator"};

    /**
     * @brief The current log level. Controls the detail of log messages.
     */
    EnumOption<LogLevel> logLevel = {LOG_LEVEL_CONFIG,
                                     LogLevel::LOG_INFO,
                                     "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)"};

    /**
     * @brief Indicates if the monitoring stack is enables.
     */
    BoolOption enableMonitoring = {ENABLE_MONITORING_CONFIG, false, "Enable monitoring"};

    /**
     * @brief Enable reconfiguration of running query plans.
     */
    BoolOption enableQueryReconfiguration = {ENABLE_QUERY_RECONFIGURATION,
                                             false,
                                             "Enable reconfiguration of running query plans. (Default: false)"};

    /**
     * @brief Configures different properties for the query optimizer.
     */
    OptimizerConfiguration optimizer = {OPTIMIZER_CONFIG, "Defines the configuration for the optimizer."};

    /**
     * @brief Allows the configuration of logical sources at the coordinator.
     * @deprecated This is currently only used for testing and will be removed.
     */
    SequenceOption<WrapOption<LogicalSourcePtr, LogicalSourceFactory>> logicalSources = {LOGICAL_SOURCES, "Logical Sources"};

    /**
     * @brief Configuration yaml path.
     * @warning this is just a placeholder configuration
     */
    StringOption configPath = {CONFIG_PATH, "", "Path to configuration file."};

    /**
     * @brief Configures different properties of the internal worker in the coordinator configuration file and on the command line.
     */
    WorkerConfiguration worker = {WORKER_CONFIG, "Defines the configuration for the worker."};

    /**
     * @brief Path to a dedicated configuration file for the internal worker.
     */
    StringOption workerConfigPath = {WORKER_CONFIG_PATH, "", "Path to a configuration file for the internal worker."};

    /**
     * @brief Configuration of waiting time of the coordinator health check.
     * Set the number of seconds waiting to perform health checks
     */
    UIntOption coordinatorHealthCheckWaitTime = {HEALTH_CHECK_WAIT_TIME, 1, "Number of seconds to wait between health checks"};

    /**
     * Create a CoordinatorConfiguration object with default values.
     * @return A CoordinatorConfiguration object with default values.
     */
    static std::shared_ptr<CoordinatorConfiguration> create() { return std::make_shared<CoordinatorConfiguration>(); }

    /**
     * Create a CoordinatorConfiguration object and set values from the POSIX command line parameters stored in argv.
     * @param argc The argc parameter given to the main function.
     * @param argv The argv parameter given to the main function.
     * @return A configured configuration object.
     */
    static CoordinatorConfigurationPtr create(const int argc, const char** argv);

  private:
    std::vector<Configurations::BaseOption*> getOptions() override {
        return {&restIp,
                &coordinatorIp,
                &rpcPort,
                &restPort,
                &dataPort,
                &logLevel,
                &enableQueryReconfiguration,
                &enableMonitoring,
                &configPath,
                &worker,
                &workerConfigPath,
                &optimizer,
                &logicalSources,
                &coordinatorHealthCheckWaitTime};
    }
};

}// namespace Configurations
}// namespace NES

#endif// NES_CORE_INCLUDE_CONFIGURATIONS_COORDINATOR_COORDINATORCONFIGURATION_HPP_

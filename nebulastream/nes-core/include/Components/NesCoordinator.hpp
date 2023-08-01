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

#ifndef NES_CORE_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define NES_CORE_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

#include <Common/Identifiers.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Services/SourceCatalogService.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <future>
#include <string>
#include <thread>
#include <vector>

namespace grpc {
class Server;
}
namespace NES {

using namespace Configurations;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class RestServer;
using RestServerPtr = std::shared_ptr<RestServer>;

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class RequestProcessorService;
using QueryRequestProcessorServicePtr = std::shared_ptr<RequestProcessorService>;

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class ReplicationService;
using ReplicationServicePtr = std::shared_ptr<ReplicationService>;

class MonitoringService;
using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class SourceCatalogService;
using SourceCatalogServicePtr = std::shared_ptr<SourceCatalogService>;

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

class AbstractHealthCheckService;
using HealthCheckServicePtr = std::shared_ptr<AbstractHealthCheckService>;

namespace Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Query

namespace UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace UDF

}// namespace Catalogs

namespace Experimental {

class MaintenanceService;
using MaintenanceServicePtr = std::shared_ptr<MaintenanceService>;

}// namespace Experimental

namespace Spatial::Index::Experimental {
class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;
}// namespace Spatial::Index::Experimental

class NesCoordinator : public detail::virtual_enable_shared_from_this<NesCoordinator>, public Exceptions::ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = detail::virtual_enable_shared_from_this<NesCoordinator>;
    using inherited1 = ErrorListener;

  public:
    explicit NesCoordinator(CoordinatorConfigurationPtr coordinatorConfig);

    /**
     * @brief dtor
     * @return
     */
    ~NesCoordinator() override;

    /**
     * @brief start rpc server: rest server, and one worker <
     * @param bool if the method should block
     */
    uint64_t startCoordinator(bool blocking);

    /**
     * @brief method to stop coordinator
     * @param force the shutdown even when queryIdAndCatalogEntryMapping are running
     * @return bool indicating success
     */
    bool stopCoordinator(bool force);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return vector of queryStatistics
    */
    std::vector<Runtime::QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief catalog method for debug use only
     * @return sourceCatalog
     */
    Catalogs::Source::SourceCatalogPtr getSourceCatalog() const { return sourceCatalog; }

    /**
     * @brief getter of replication service
     * @return replication service
     */
    ReplicationServicePtr getReplicationService() const { return replicationService; }

    TopologyPtr getTopology() const { return topology; }

    /**
     * @brief Get the instance of query service
     * @return Query service pointer
     */
    QueryServicePtr getQueryService();

    /**
     * @brief Get instance of query catalog
     * @return query catalog pointer
     */
    QueryCatalogServicePtr getQueryCatalogService();

    /**
     * @brief Return the UDF catalog.
     * @return Pointer to the UDF catalog.
     */
    Catalogs::UDF::UdfCatalogPtr getUdfCatalog();

    /**
     * @brief Get instance of monitoring service
     * @return monitoring service pointer
     */
    MonitoringServicePtr getMonitoringService();

    /**
     * @brief Get instance of maintenance service
     * @return maintenance service pointer
     */
    NES::Experimental::MaintenanceServicePtr getMaintenanceService();

    /**
     * @brief Get the instance of Global Query Plan
     * @return Global query plan
     */
    GlobalQueryPlanPtr getGlobalQueryPlan();

    Runtime::NodeEnginePtr getNodeEngine();

    void onFatalError(int signalNumber, std::string string) override;
    void onFatalException(std::shared_ptr<std::exception> ptr, std::string string) override;

    /**
     * @brief Method to check if a coordinator is still running
     * @return running status of the coordinator
     */
    bool isCoordinatorRunning();

    /**
     * getter for the sourceCatalogService
     * @return
     */
    SourceCatalogServicePtr getSourceCatalogService() const;

    /**
     * getter for the topologyManagerService
     * @return
     */
    TopologyManagerServicePtr getTopologyManagerService() const;

    /**
     * getter for the locationService
     * @return
     */
    NES::Spatial::Index::Experimental::LocationServicePtr getLocationService() const;

    NesWorkerPtr getNesWorker();

  private:
    /**
     * @brief this method will start the GRPC Coordinator server which is responsible for reacting to calls from the CoordinatorRPCClient
     */
    void buildAndStartGRPCServer(const std::shared_ptr<std::promise<bool>>& prom);

    CoordinatorConfigurationPtr coordinatorConfiguration;
    std::string restIp;
    uint16_t restPort;
    std::string rpcIp;
    uint16_t rpcPort;
    std::unique_ptr<grpc::Server> rpcServer;
    std::shared_ptr<std::thread> rpcThread;
    std::shared_ptr<std::thread> queryRequestProcessorThread;
    NesWorkerPtr worker;
    TopologyManagerServicePtr topologyManagerService;
    SourceCatalogServicePtr sourceCatalogService;
    HealthCheckServicePtr healthCheckService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    QueryCatalogServicePtr queryCatalogService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::Query::QueryCatalogPtr queryCatalog;
    TopologyPtr topology;
    RestServerPtr restServer;
    std::shared_ptr<std::thread> restThread;
    std::atomic<bool> isRunning{false};
    QueryRequestProcessorServicePtr queryRequestProcessorService;
    QueryServicePtr queryService;
    MonitoringServicePtr monitoringService;
    ReplicationServicePtr replicationService;
    NES::Experimental::MaintenanceServicePtr maintenanceService;
    WorkerRPCClientPtr workerRpcClient;
    RequestQueuePtr queryRequestQueue;
    GlobalQueryPlanPtr globalQueryPlan;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    bool enableMonitoring;
    NES::Spatial::Index::Experimental::LocationServicePtr locationService;

  public:
    constexpr static uint64_t NES_COORDINATOR_ID = 1;
};
using NesCoordinatorPtr = std::shared_ptr<NesCoordinator>;

}// namespace NES
#endif// NES_CORE_INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_

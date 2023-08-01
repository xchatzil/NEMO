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

#include <Monitoring/MonitoringManager.hpp>
#include <Util/Logger/Logger.hpp>

#include <Components/NesCoordinator.hpp>

#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/Metrics/Gauge/CpuMetrics.hpp>
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>
#include <Monitoring/Metrics/Gauge/NetworkMetrics.hpp>
#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/NodeEngine.hpp>

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryService.hpp>

#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/QueryStatus.hpp>
#include <Util/UtilityFunctions.hpp>
#include <regex>
#include <utility>

#include <nlohmann/json.hpp>

namespace NES::Monitoring {
MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient,
                                     TopologyPtr topology,
                                     QueryServicePtr queryService,
                                     QueryCatalogServicePtr catalogService)
    : MonitoringManager(workerClient, topology, queryService, catalogService, true) {}

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient,
                                     TopologyPtr topology,
                                     QueryServicePtr queryService,
                                     QueryCatalogServicePtr catalogService,
                                     bool enableMonitoring)
    : MonitoringManager(workerClient,
                        topology,
                        queryService,
                        catalogService,
                        std::make_shared<LatestEntriesMetricStore>(),
                        enableMonitoring) {}

MonitoringManager::MonitoringManager(WorkerRPCClientPtr workerClient,
                                     TopologyPtr topology,
                                     QueryServicePtr queryService,
                                     QueryCatalogServicePtr catalogService,
                                     MetricStorePtr metricStore,
                                     bool enableMonitoring)
    : metricStore(metricStore), workerClient(workerClient), topology(topology), enableMonitoring(enableMonitoring),
      monitoringCollectors(MonitoringPlan::defaultCollectors()) {
    this->queryService = queryService;
    this->catalogService = catalogService;
    NES_DEBUG("MonitoringManager: Init with monitoring=" << enableMonitoring << ", storage=" << toString(metricStore->getType()));
}

MonitoringManager::~MonitoringManager() {
    NES_DEBUG("MonitoringManager: Shutting down");
    workerClient.reset();
    topology.reset();
}

bool MonitoringManager::registerRemoteMonitoringPlans(const std::vector<uint64_t>& nodeIds, MonitoringPlanPtr monitoringPlan) {
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Register plan failed. Monitoring is disabled.");
        return false;
    }
    if (!monitoringPlan) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no plan is provided.");
        return false;
    }
    if (nodeIds.empty()) {
        NES_ERROR("MonitoringManager: Register monitoring plan failed, no nodes are provided.");
        return false;
    }

    for (auto nodeId : nodeIds) {
        NES_DEBUG("MonitoringManager: Registering monitoring plan for worker id= " + std::to_string(nodeId));
        TopologyNodePtr node = topology->findNodeWithId(nodeId);

        if (node) {
            auto nodeIp = node->getIpAddress();
            auto nodeGrpcPort = node->getGrpcPort();
            std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

            auto success = workerClient->registerMonitoringPlan(destAddress, monitoringPlan);

            if (success) {
                NES_DEBUG("MonitoringManager: Node with ID " + std::to_string(nodeId) + " registered successfully.");
                monitoringPlanMap[nodeId] = monitoringPlan;
            } else {
                NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " failed to register plan over GRPC.");
                return false;
            }
        } else {
            NES_ERROR("MonitoringManager: Node with ID " + std::to_string(nodeId) + " does not exit.");
            return false;
        }
    }
    return true;
}

nlohmann::json MonitoringManager::requestRemoteMonitoringData(uint64_t nodeId) {
    nlohmann::json metricsJson;
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Requesting monitoring data for node " << nodeId
                                                                            << " failed. Monitoring is disabled, "
                                                                               "returning empty object");
        return metricsJson;
    }

    NES_DEBUG("MonitoringManager: Requesting metrics for node id=" + std::to_string(nodeId));
    auto plan = getMonitoringPlan(nodeId);

    //getMonitoringPlan(..) checks if node exists, so no further check necessary
    TopologyNodePtr node = topology->findNodeWithId(nodeId);
    auto nodeIp = node->getIpAddress();
    auto nodeGrpcPort = node->getGrpcPort();
    std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);
    auto metricsAsJsonString = workerClient->requestMonitoringData(destAddress);

    if (!metricsAsJsonString.empty()) {
        NES_DEBUG("MonitoringManager: Received monitoring data " + metricsAsJsonString);
        //convert string to json object
        metricsJson = metricsJson.parse(metricsAsJsonString);
        return metricsJson;
    }
    NES_THROW_RUNTIME_ERROR("MonitoringManager: Error receiving monitoring metrics for node with id "
                            + std::to_string(node->getId()));
}

StoredNodeMetricsPtr MonitoringManager::getMonitoringDataFromMetricStore(uint64_t node) {
    return metricStore->getAllMetrics(node);
}

void MonitoringManager::addMonitoringData(uint64_t nodeId, MetricPtr metrics) {
    NES_TRACE("MonitoringManager: Adding metrics of type " << toString(metrics->getMetricType()) << " for node " << nodeId
                                                           << " in store " << metricStore);
    metricStore->addMetrics(nodeId, metrics);
}

void MonitoringManager::removeMonitoringNode(uint64_t nodeId) {
    NES_DEBUG("MonitoringManager: Removing node and metrics for node " << nodeId);
    monitoringPlanMap.erase(nodeId);
    metricStore->removeMetrics(nodeId);
}

MonitoringPlanPtr MonitoringManager::getMonitoringPlan(uint64_t nodeId) {
    if (monitoringPlanMap.find(nodeId) == monitoringPlanMap.end()) {
        TopologyNodePtr node = topology->findNodeWithId(nodeId);
        if (node) {
            NES_DEBUG("MonitoringManager: No registered plan found. Returning default plan for node " + std::to_string(nodeId));
            return MonitoringPlan::defaultPlan();
        }
        NES_THROW_RUNTIME_ERROR("MonitoringManager: Retrieving metrics for " + std::to_string(nodeId)
                                + " failed. Node does not exist in topology.");
    } else {
        return monitoringPlanMap[nodeId];
    }
}

MetricStorePtr MonitoringManager::getMetricStore() { return metricStore; }

bool MonitoringManager::registerLogicalMonitoringStreams(const NES::Configurations::CoordinatorConfigurationPtr config) {
    if (enableMonitoring) {
        for (auto collectorType : monitoringCollectors) {
            auto metricSchema = MetricUtils::getSchemaFromCollectorType(collectorType);
            // auto generate the specifics
            MetricType metricType = MetricUtils::createMetricFromCollectorType(collectorType)->getMetricType();
            std::string logicalSourceName = NES::Monitoring::toString(metricType);
            logicalMonitoringSources.insert(logicalSourceName);
            NES_INFO("MonitoringManager: Creating logical source " << logicalSourceName);
            config->logicalSources.add(LogicalSource::create(logicalSourceName, metricSchema));
        }
        return true;
    }
    NES_WARNING("MonitoringManager: Monitoring is disabled, registering of logical monitoring streams not possible.");
    return false;
}

QueryId MonitoringManager::startOrRedeployMonitoringQuery(std::string monitoringStream, bool sync) {
    QueryId queryId = 0;

    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
    }

    bool success;
    if (deployedMonitoringQueries.contains(monitoringStream)) {
        success = stopRunningMonitoringQuery(monitoringStream, sync);
    } else {
        success = true;
    }

    // params for iteration
    if (success) {
        MetricType metricType = parse(monitoringStream);
        std::string metricCollectorStr = NES::Monitoring::toString(MetricUtils::createCollectorTypeFromMetricType(metricType));
        std::string query =
            R"(Query::from("%STREAM%").sink(MonitoringSinkDescriptor::create(Monitoring::MetricCollectorType::%COLLECTOR%));)";
        query = std::regex_replace(query, std::regex("%STREAM%"), monitoringStream);
        query = std::regex_replace(query, std::regex("%COLLECTOR%"), metricCollectorStr);

        // create new monitoring query
        NES_INFO("MonitoringManager: Creating query for " << monitoringStream);
        queryId =
            queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
        if ((sync && waitForQueryToStart(queryId, std::chrono::seconds(60))) || (!sync)) {
            NES_INFO("MonitoringManager: Successfully started query " << queryId << "::" << monitoringStream);
            deployedMonitoringQueries.insert({monitoringStream, queryId});
        } else {
            NES_ERROR("MonitoringManager: Query " << queryId << "::" << monitoringStream << " failed to start in time.");
        }
    } else {
        NES_ERROR("MonitoringManager: Failed to deploy monitoring query. Queries are still running and could not be stopped for "
                  << monitoringStream);
    }
    return queryId;
}

std::unordered_map<std::string, QueryId> MonitoringManager::startOrRedeployMonitoringQueries(bool sync) {
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
        return deployedMonitoringQueries;
    }

    // params for iteration
    for (auto monitoringStream : logicalMonitoringSources) {
        bool success = stopRunningMonitoringQuery(monitoringStream, sync);

        if (success) {
            startOrRedeployMonitoringQuery(monitoringStream, sync);
        }
    }

    return deployedMonitoringQueries;
}

bool MonitoringManager::stopRunningMonitoringQuery(std::string streamName, bool sync) {
    bool success = true;

    // params for iteration
    if (deployedMonitoringQueries.contains(streamName)) {
        auto metricType = streamName;
        auto queryId = deployedMonitoringQueries[streamName];

        NES_INFO("MonitoringManager: Stopping query " << queryId << " for " << metricType);
        if (queryService->validateAndQueueStopQueryRequest(queryId)) {
            if ((sync && checkStoppedOrTimeout(queryId, std::chrono::seconds(60))) || (!sync)) {
                NES_INFO("MonitoringManager: Query " << queryId << "::" << metricType << " terminated.");
            } else {
                NES_ERROR("MonitoringManager: Failed to stop query " << queryId << "::" << metricType);
                success = false;
            }
        } else {
            NES_ERROR("MonitoringManager: Failed to validate query " << queryId << "::" << metricType);
            success = false;
        }
    }
    if (success) {
        deployedMonitoringQueries.erase(streamName);
        NES_INFO("MonitoringManager: Monitoring query stopped successfully " << streamName);
    }
    return success;
}

bool MonitoringManager::stopRunningMonitoringQueries(bool sync) {
    bool success = true;
    if (!enableMonitoring) {
        NES_ERROR("MonitoringManager: Deploying queries failed. Monitoring is disabled.");
    }

    // params for iteration
    for (auto monitoringStream : logicalMonitoringSources) {
        if (!stopRunningMonitoringQuery(monitoringStream, sync)) {
            success = false;
        }
    }

    return success;
}

bool MonitoringManager::checkStoppedOrTimeout(QueryId queryId, std::chrono::seconds timeout) {
    auto timeoutInSec = std::chrono::seconds(timeout);
    auto start_timestamp = std::chrono::system_clock::now();
    while (std::chrono::system_clock::now() < start_timestamp + timeoutInSec) {
        NES_TRACE("checkStoppedOrTimeout: check query status for " << queryId);
        if (catalogService->getEntryForQuery(queryId)->getQueryStatus() == QueryStatus::Stopped) {
            NES_TRACE("checkStoppedOrTimeout: status for " << queryId << " reached stopped");
            return true;
        }
        NES_DEBUG("checkStoppedOrTimeout: status not reached for "
                  << queryId << " as status is=" << catalogService->getEntryForQuery(queryId)->getQueryStatusAsString());
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_TRACE("checkStoppedOrTimeout: expected status not reached within set timeout");
    return false;
}

bool MonitoringManager::waitForQueryToStart(QueryId queryId, std::chrono::seconds timeout) {
    NES_INFO("MonitoringManager: wait till the query " << queryId << " gets into Running status.");
    auto start_timestamp = std::chrono::system_clock::now();

    while (std::chrono::system_clock::now() < start_timestamp + timeout) {
        auto queryCatalogEntry = catalogService->getEntryForQuery(queryId);
        if (!queryCatalogEntry) {
            NES_ERROR("MonitoringManager: unable to find the entry for query " << queryId << " in the query catalog.");
            return false;
        }
        NES_TRACE("MonitoringManager: Query " << queryId << " is now in status " << queryCatalogEntry->getQueryStatusAsString());
        QueryStatus::Value status = queryCatalogEntry->getQueryStatus();

        switch (queryCatalogEntry->getQueryStatus()) {
            case QueryStatus::Running: {
                NES_DEBUG("MonitoringManager: Query is now running " << queryId);
                return true;
            }
            default: {
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }
    NES_ERROR("MonitoringManager: Starting query " << queryId << " has timed out.");
    return false;
}

bool MonitoringManager::isMonitoringStream(std::string streamName) const { return logicalMonitoringSources.contains(streamName); }

const std::unordered_map<std::string, QueryId>& MonitoringManager::getDeployedMonitoringQueries() const {
    return deployedMonitoringQueries;
}

}// namespace NES::Monitoring
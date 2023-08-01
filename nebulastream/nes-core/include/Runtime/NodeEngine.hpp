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

#ifndef NES_CORE_INCLUDE_RUNTIME_NODEENGINE_HPP_
#define NES_CORE_INCLUDE_RUNTIME_NODEENGINE_HPP_

#include <Common/ForwardDeclaration.hpp>
#include <Common/Identifiers.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Network/ExchangeProtocolListener.hpp>
#include <Network/NetworkForwardRefs.hpp>
#include <Runtime/MaterializedViewManager.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <iostream>
#include <pthread.h>
#include <string>
#include <unistd.h>
#include <unordered_set>
#include <vector>

namespace NES {

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Monitoring {
class AbstractMetricStore;
using MetricStorePtr = std::shared_ptr<AbstractMetricStore>;
}//namespace Monitoring

namespace Runtime {

/**
 * @brief this class represents the interface and entrance point into the
 * query processing part of NES. It provides basic functionality
 * such as deploying, undeploying, starting, and stopping.
 *
 */
class NodeEngine : public Network::ExchangeProtocolListener,
                   public NES::detail::virtual_enable_shared_from_this<NodeEngine>,
                   public Exceptions::ErrorListener {
    // virtual_enable_shared_from_this necessary for double inheritance of enable_shared_from_this
    using inherited0 = Network::ExchangeProtocolListener;
    using inherited1 = virtual_enable_shared_from_this<NodeEngine>;
    using inherited2 = ErrorListener;

    friend class NodeEngineBuilder;

  public:
    enum NodeEngineQueryStatus { started, stopped, registered };

    virtual ~NodeEngine() override;

    NodeEngine() = delete;
    NodeEngine(const NodeEngine&) = delete;
    NodeEngine& operator=(const NodeEngine&) = delete;

    /**
     * @brief signal handler: behaviour not clear yet!
     * @param signalNumber
     * @param callstack
     */
    void onFatalError(int signalNumber, std::string callstack) override;

    /**
     * @brief exception handler: behaviour not clear yet!
     * @param exception
     * @param callstack
     */
    void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override;

    /**
     * @brief deploy registers and starts a query
     * @param new query plan
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool deployQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);

    /**
     * @brief undeploy stops and undeploy a query
     * @param queryId to undeploy
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool undeployQuery(QueryId queryId);

    /**
     * @brief registers a query
     * @param query plan to register
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerQueryInNodeEngine(const Execution::ExecutableQueryPlanPtr& queryExecutionPlan);

    /**
     * @brief registers a query
     * @param queryId: id of the query sub plan to be registered
     * @param queryExecutionId: query execution plan id
     * @param operatorTree: query sub plan to register
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool registerQueryInNodeEngine(const QueryPlanPtr& queryPlan);

    /**
     * @brief ungregisters a query
     * @param queryIdto unregister query
     * @return true if succeeded, else false
     */
    [[nodiscard]] bool unregisterQuery(QueryId queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    [[nodiscard]] bool startQuery(QueryId queryId);

    /**
     * @brief method to stop a query
     * @param queryId to stop
     * @param graceful hard or soft termination
     * @return bool indicating success
     */
    [[nodiscard]] bool stopQuery(QueryId queryId,
                                 Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::HardStop);

    /**
     * @brief method to trigger the buffering of data on a NetworkSink of a Query Sub Plan with the given id
     * @param querySubPlanId : the id of the Query Sub Plan to which the Network Sink belongs to
     * @param uniqueNetworkSinkDescriptorId : the id of the Network Sink Descriptor. Helps identify the Network Sink on which to buffer data
     * @return bool indicating success
     */
    bool bufferData(QuerySubPlanId querySubPlanId, uint64_t uniqueNetworkSinkDescriptorId);

    /**
     * @brief method to trigger the buffering of data on all NetworkSinks of this Node.
     * This function has no effect any NetworkSinks faultToleranceType is set to AT_LEAST_ONCE as it might cause
     * threading problems to allow reconnect buffering in this case.
     * @return bool indicating succes
     */
    bool bufferAllData();

    /**
     * @brief unbuffer all tuples that were buffered since bufferAllData was called and write them to the respective sinks
     * and stop buffering new incoming tuples. This might change the order in which tuples are sent over the network channel if
     * new tuples are coming in while unbuffering is in process
     * @return bool indicating success
     */
    bool stopBufferingAllData();

    /**
     * @brief method to trigger the reconfiguration of a NetworkSink so that it points to a new downstream node.
     * @param newNodeId : the id of the new node
     * @param newHostname : the hostname of the new node
     * @param newPort : the port of the new node
     * @param querySubPlanId : the id of the Query Sub Plan to which the Network Sink belongs to
     * @param uniqueNetworkSinkDescriptorId : the id of the Network Sink Descriptor. Helps identify the Network Sink to reconfigure.
     * @return bool indicating success
     */
    bool updateNetworkSink(uint64_t newNodeId,
                           const std::string& newHostname,
                           uint32_t newPort,
                           QuerySubPlanId querySubPlanId,
                           uint64_t uniqueNetworkSinkDescriptorId);

    /**
     * @brief release all resource of the node engine
     * @param withError true if the node engine stopped with an error
     */
    [[nodiscard]] bool stop(bool markQueriesAsFailed = false);

    /**
     * @brief getter of query manager
     * @return query manager
     */
    QueryManagerPtr getQueryManager();

    /**
     * @brief getter of buffer manager for the i-th numa region (defaul: 0)
     * @return bufferManager
     */
    BufferManagerPtr getBufferManager(uint32_t bufferManagerIndex = 0) const;

    /**
    * @brief getter of state manager
    * @return stateManager
    */
    StateManagerPtr getStateManager();

    /**
    * @brief getter of node id
    * @return NodeEngineId
    */
    uint64_t getNodeEngineId();

    /**
     * @brief getter of network manager
     * @return network manager
     */
    Network::NetworkManagerPtr getNetworkManager();

    /**
     * @brief getter of query status listener
     * @return return the query status listener
     */
    AbstractQueryStatusListenerPtr getQueryStatusListener();

    /**
     * @return return the status of a query
     */
    Execution::ExecutableQueryPlanStatus getQueryStatus(QueryId queryId);

    /**
     * @brief method to inject new epoch timestamp to data stream
     * @param timestamp: max timestamp of current epoch
     * @param queryId: identifies what query sends punctuation
     */
    void injectEpochBarrier(uint64_t timestamp, uint64_t queryId) const;

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return vector of queryStatistics
    */
    std::vector<QueryStatisticsPtr> getQueryStatistics(QueryId queryId);

    /**
     * @brief method to return the query statistics
     * @param withReset specifies if the statistics is deleted after reading (so we start with 0)
     * @return vector of queryStatistics
    */
    std::vector<QueryStatistics> getQueryStatistics(bool withReset = false);

    Network::PartitionManagerPtr getPartitionManager();

    /**
     * @brief getter of materialized view manager
     * @return materialized view manager
     */
    NES::Experimental::MaterializedView::MaterializedViewManagerPtr getMaterializedViewManager() const;

    ///// Network Callback //////

    /**
     * @brief this callback is called once a tuple buffer arrives on the network manager
     * for a given nes partition
     */
    void onDataBuffer(Network::NesPartition, TupleBuffer&) override;

    /**
     * @brief this callback is called once a tuple buffer arrives on the network manager
     * for a given nes partition
     */
    void onEvent(Network::NesPartition, Runtime::BaseEvent&) override;

    /**
     * @brief this callback is called once an end of stream message arrives
     */
    void onEndOfStream(Network::Messages::EndOfStreamMessage) override;

    /**
     * @brief this callback is called once an error is raised on the server side
     */
    void onServerError(Network::Messages::ErrorMessage) override;

    /**
     * @brief this callback is called once an error is raised on the channel(client) side
     */
    void onChannelError(Network::Messages::ErrorMessage) override;

    /**
     * @brief Provide the hardware manager
     * @return the hardware manager
     */
    HardwareManagerPtr getHardwareManager() const;

    /**
     * @brief Get physical sources configured
     * @return list of physical sources
     */
    const std::vector<PhysicalSourcePtr>& getPhysicalSources() const;

    /**
     * @brief finds executable query plan for a given sub query id
     * @param querySubPlanId query sub plan id
     * @return executable query plan
     */
    std::shared_ptr<const Execution::ExecutableQueryPlan> getExecutableQueryPlan(uint64_t querySubPlanId) const;

    /**
     * @brief finds sub query ids for a given query id
     * @param queryId query id
     * @return vector of subQueryIds
     */
    std::vector<QuerySubPlanId> getSubQueryIds(uint64_t queryId);

    /**
     * Getter for the metric store
     * @return the metric store
     */
    Monitoring::MetricStorePtr getMetricStore();

    /**
     * Setter for the metric store
     * @param metricStore
     */
    void setMetricStore(Monitoring::MetricStorePtr metricStore);

    /**
     * Getter for node Id
     * @return the node id
     */
    TopologyNodeId getNodeId() const;

    /**
     * Setter for node ID
     * @param NodeId
     */
    void setNodeId(const TopologyNodeId NodeId);

    /**
     * @brief Updates the physical sources on the node engine
     * @param physicalSources
     */
    void updatePhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources);

  public:
    /**
     * @brief Create a node engine and gather node information
     * and initialize QueryManager, BufferManager and ThreadPool
     */
    explicit NodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                        HardwareManagerPtr&&,
                        std::vector<BufferManagerPtr>&&,
                        QueryManagerPtr&&,
                        std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&&,
                        Network::PartitionManagerPtr&&,
                        QueryCompilation::QueryCompilerPtr&&,
                        StateManagerPtr&&,
                        std::weak_ptr<AbstractQueryStatusListener>&&,
                        NES::Experimental::MaterializedView::MaterializedViewManagerPtr&&,
                        uint64_t nodeEngineId,
                        uint64_t numberOfBuffersInGlobalBufferManager,
                        uint64_t numberOfBuffersInSourceLocalBufferPool,
                        uint64_t numberOfBuffersPerWorker,
                        bool sourceSharing);

  private:
    TopologyNodeId nodeId;
    std::vector<PhysicalSourcePtr> physicalSources;
    std::map<QueryId, std::vector<QuerySubPlanId>> queryIdToQuerySubPlanIds;
    std::map<QuerySubPlanId, Execution::ExecutableQueryPlanPtr> deployedQEPs;
    HardwareManagerPtr hardwareManager;
    std::vector<BufferManagerPtr> bufferManagers;
    QueryManagerPtr queryManager;
    BufferStoragePtr bufferStorage;
    Monitoring::MetricStorePtr metricStore;
    QueryCompilation::QueryCompilerPtr queryCompiler;
    Network::PartitionManagerPtr partitionManager;
    StateManagerPtr stateManager;
    AbstractQueryStatusListenerPtr nesWorker;
    Network::NetworkManagerPtr networkManager;
    NES::Experimental::MaterializedView::MaterializedViewManagerPtr materializedViewManager;
    std::atomic<bool> isRunning{};
    mutable std::recursive_mutex engineMutex;
    [[maybe_unused]] uint64_t nodeEngineId;
    [[maybe_unused]] uint32_t numberOfBuffersInGlobalBufferManager;
    [[maybe_unused]] uint32_t numberOfBuffersInSourceLocalBufferPool;
    [[maybe_unused]] uint32_t numberOfBuffersPerWorker;
    bool sourceSharing;
};

using NodeEnginePtr = std::shared_ptr<NodeEngine>;

}// namespace Runtime
}// namespace NES
#endif// NES_CORE_INCLUDE_RUNTIME_NODEENGINE_HPP_

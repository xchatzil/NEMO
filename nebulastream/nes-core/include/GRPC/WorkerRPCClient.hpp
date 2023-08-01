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

#ifndef NES_CORE_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_
#define NES_CORE_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

#include <Common/Identifiers.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/TimeMeasurement.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <WorkerRPCService.pb.h>
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Monitoring {
class MonitoringPlan;

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;
}// namespace Monitoring

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

using CompletionQueuePtr = std::shared_ptr<CompletionQueue>;

namespace Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
class Waypoint;
using WaypointPtr = std::shared_ptr<Waypoint>;
}// namespace Spatial::Index::Experimental

namespace Spatial::Mobility::Experimental {
class ReconnectSchedule;
using ReconnectSchedulePtr = std::shared_ptr<ReconnectSchedule>;
}// namespace Spatial::Mobility::Experimental

enum RpcClientModes { Register, Unregister, Start, Stop };

class WorkerRPCClient {
  public:
    template<typename ReplayType>
    struct AsyncClientCall {
        // Container for the data we expect from the server.
        ReplayType reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // Storage for the status of the RPC upon completion.
        Status status;

        std::unique_ptr<ClientAsyncResponseReader<ReplayType>> responseReader;
    };

    WorkerRPCClient() = default;

    void AsyncCompleteRpc();

    /**
        * @brief register a query
        * @param address: address of node where query plan need to be registered
        * @param query plan to register
        * @return true if succeeded, else false
        */
    static bool registerQuery(const std::string& address, const QueryPlanPtr& queryPlan);

    /**
    * @brief register a query asynchronously
    * @param address: address of node where query plan need to be registered
    * @param query plan to register
    * @return true if succeeded, else false
    */
    static bool registerQueryAsync(const std::string& address, const QueryPlanPtr& queryPlan, const CompletionQueuePtr& cq);

    /**
     * @brief ungregisters a query
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    static bool unregisterQuery(const std::string& address, QueryId queryId);

    /**
     * @brief ungregisters a query asynchronously
     * @param queryId to unregister query
     * @return true if succeeded, else false
     */
    static bool unregisterQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId to start
     * @return bool indicating success
     */
    static bool startQuery(const std::string& address, QueryId queryId);

    /**
      * @brief method to start a already deployed query asynchronously
      * @note if query is not deploy, false is returned
      * @param queryId to start
      * @return bool indicating success
      */
    static bool startQueryAsyn(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq);

    /**
     * @brief method to stop a query
     * @param address address of the new worker
     * @param queryId to stop
     * @param terminationType termination type of the query
     * @return bool indicating success
     */
    static bool stopQuery(const std::string& address, QueryId queryId, Runtime::QueryTerminationType terminationType);

    /**
     * @brief method to stop a query asynchronously
     * @param address : address of the worker
     * @param queryId to stop
     * @param terminationType: the termination type
     * @param cq: completion queue of grpc requests
     * @return bool indicating success
     */
    static bool stopQueryAsync(const std::string& address,
                               QueryId queryId,
                               Runtime::QueryTerminationType terminationType,
                               const CompletionQueuePtr& cq);

    /**
     * @brief Registers to a remote worker node its monitoring plan.
     * @param ipAddress
     * @param the monitoring plan
     * @return bool if successful
     */
    static bool registerMonitoringPlan(const std::string& address, const Monitoring::MonitoringPlanPtr& plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @return true if successful, else false
     */
    static std::string requestMonitoringData(const std::string& address);

    /**
     * @brief Requests remote worker to start buffering data on a single NetworkSink identified by
     * a query sub plan Id and a global sinkId.
     * Once buffering starts, the Network Sink no longer sends data downstream
     * @param ipAddress
     * @param querySubPlanId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    static bool bufferData(const std::string& address, uint64_t querySubPlanId, uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief requests a remote worker to reconfigure a NetworkSink so that the NetworkSink changes where it sends data to (changes downstream node)
     * @param ipAddress
     * @param newNodeId : the id of the node that the Network Sink will send data to after reconfiguration
     * @param newHostname : the hostname of the node that the NetworkSink should send data to
     * @param newPort : the port of the node that the NetworkSink should send data to
     * @param querySubPlanId : the id of the query sub plan to which the Network Sink belongs
     * @param uniqueNetworkSinDescriptorId : unique id of the network sink descriptor. Used to find the Network Sink to buffer data on.
     * @return true if successful, else false
     */
    static bool updateNetworkSink(const std::string& address,
                                  uint64_t newNodeId,
                                  const std::string& newHostname,
                                  uint32_t newPort,
                                  uint64_t querySubPlanId,
                                  uint64_t uniqueNetworkSinDescriptorId);

    /**
     * @brief This functions loops over all queues and wait for the async calls return
     * @param queues
     * @param mode
     * @return true if all calls returned
     */
    static bool checkAsyncResult(const std::map<CompletionQueuePtr, uint64_t>& queues, RpcClientModes mode);

    /**
     * @brief method to propagate new epoch timestamp to source
     * @param timestamp: max timestamp of current epoch
     * @param queryId: query id which sources belong to
     * @param address: ip address of the source
     * @return bool indicating success
     */
    bool injectEpochBarrier(uint64_t timestamp, uint64_t queryId, const std::string& address);

    /**
     * @brief method to check the health of the worker
     * @param address: ip address of the source
     * @return bool indicating success
     */
    bool checkHealth(const std::string& address, std::string healthServiceName);

    /**
     * @brief method to check the location of any node. If the node is a mobile node, its current loction will be returned.
     * If the node is a field node, its fixed location will be returned. If the node does not have a known location, an
     * invalid location will be returned
     * @param address: the ip adress of the node
     * @return location representing the nodes location or invalid if no such location exists
     */
    static NES::Spatial::Index::Experimental::WaypointPtr getWaypoint(const std::string& address);

    /**
     * @brief method to obtain the reconnect schedule calculated by a mobile worker containing beginning and end locations of the
     * mobile workers predicted path, the position of the last node index update at the device side and a vector containing the
     * scheduled reconnects. If the device is not a mobile device, then the returned schedule will have all its members set to
     * nullptr
     * @param address: the ip adress of the node
     * @return a ReconnectSchedule object
     */
    static NES::Spatial::Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule(const std::string& address);

  private:
};
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_WORKERRPCCLIENT_HPP_

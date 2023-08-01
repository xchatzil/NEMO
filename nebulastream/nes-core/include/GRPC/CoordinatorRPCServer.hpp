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

#ifndef NES_CORE_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_
#define NES_CORE_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_

#include <CoordinatorRPCService.grpc.pb.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

namespace NES {

class QueryService;
using QueryServicePtr = std::shared_ptr<QueryService>;

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

class SourceCatalogService;
using SourceCatalogServicePtr = std::shared_ptr<SourceCatalogService>;

class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

namespace Monitoring {
class MonitoringManager;
using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;
}// namespace Monitoring

class ReplicationService;
using ReplicationServicePtr = std::shared_ptr<ReplicationService>;

namespace Spatial::Index::Experimental {
class LocationService;
using LocationServicePtr = std::shared_ptr<LocationService>;
}// namespace Spatial::Index::Experimental

/**
 * @brief Coordinator RPC server responsible for receiving requests over GRPC interface
 */
class CoordinatorRPCServer final : public CoordinatorRPCService::Service {
  public:
    /**
     * @brief Create coordinator RPC server
     * @param queryService: the instance of Query Service
     * @param topologyManagerService : the instance of the topologyManagerService
     * @param sourceCatalogService : the instance of the steam catalog service
     * @param queryCatalogService : the instance of monitoring service
     * @param monitoringService : the instance of monitoring service
     * @param replicationService : the instance of monitoring service
     */
    explicit CoordinatorRPCServer(QueryServicePtr queryService,
                                  TopologyManagerServicePtr topologyManagerService,
                                  SourceCatalogServicePtr sourceCatalogService,
                                  QueryCatalogServicePtr queryCatalogService,
                                  Monitoring::MonitoringManagerPtr monitoringManager,
                                  ReplicationServicePtr replicationService,
                                  NES::Spatial::Index::Experimental::LocationServicePtr locationService);
    /**
     * @brief RPC Call to register a node
     * @param context: the server context
     * @param request: node registration request
     * @param reply: the node registration reply
     * @return success
     */
    Status RegisterNode(ServerContext* context, const RegisterNodeRequest* request, RegisterNodeReply* reply) override;

    /**
     * @brief RPC Call to unregister a node
     * @param context: the server context
     * @param request: node unregistration request
     * @param reply: the node unregistration reply
     * @return success
     */
    Status UnregisterNode(ServerContext* context, const UnregisterNodeRequest* request, UnregisterNodeReply* reply) override;

    /**
     * @brief RPC Call to register physical source
     * @param context: the server context
     * @param request: register physical source request
     * @param reply: register physical source response
     * @return success
     */
    Status RegisterPhysicalSource(ServerContext* context,
                                  const RegisterPhysicalSourcesRequest* request,
                                  RegisterPhysicalSourcesReply* reply) override;

    /**
     * @brief RPC Call to unregister physical source
     * @param context: the server context
     * @param request: unregister physical source request
     * @param reply: unregister physical source reply
     * @return success
     */
    Status UnregisterPhysicalSource(ServerContext* context,
                                    const UnregisterPhysicalSourceRequest* request,
                                    UnregisterPhysicalSourceReply* reply) override;

    /**
     * @brief RPC Call to register logical source
     * @param context: the server context
     * @param request: register logical source request
     * @param reply: register logical source response
     * @return success
     */
    Status RegisterLogicalSource(ServerContext* context,
                                 const RegisterLogicalSourceRequest* request,
                                 RegisterLogicalSourceReply* reply) override;

    /**
     * @brief RPC Call to unregister logical source
     * @param context: the server context
     * @param request: unregister logical source request
     * @param reply: unregister logical source response
     * @return success
     */
    Status UnregisterLogicalSource(ServerContext* context,
                                   const UnregisterLogicalSourceRequest* request,
                                   UnregisterLogicalSourceReply* reply) override;

    /**
     * @brief RPC Call to add parent
     * @param context: the server context
     * @param request: add parent request
     * @param reply: add parent reply
     * @return success
     */
    Status AddParent(ServerContext* context, const AddParentRequest* request, AddParentReply* reply) override;

    /**
     * @brief RPC Call to replace parent
     * @param context: the server context
     * @param request: replace parent request
     * @param reply: replace parent reply
     * @return success
     */
    Status ReplaceParent(ServerContext* context, const ReplaceParentRequest* request, ReplaceParentReply* reply) override;

    /**
     * @brief RPC Call to remove parent
     * @param context: the server context
     * @param request: remove parent request
     * @param reply: remove parent response
     * @return success
     */
    Status RemoveParent(ServerContext* context, const RemoveParentRequest* request, RemoveParentReply* reply) override;

    /**
     * @brief RPC Call to notify the failure of a query
     * @param context: the server context
     * @param request that is sent from worker to the coordinator and filled with information of the failed query (Ids of query, worker, etc. and error message)
     * @param reply that is sent back from the coordinator to the worker to confirm that notification was successful
     * @return success
     */
    Status NotifyQueryFailure(ServerContext* context,
                              const QueryFailureNotification* request,
                              QueryFailureNotificationReply* reply) override;

    /**
      * @brief RPC Call to propagate timestamp
      * @param context
      * @param request that is sent from worker to the coordinator, contains timestamp and queryId
      * @param reply that is sent back from the coordinator to the worker to confirm that notification was successful
      * @return success
      */
    Status NotifyEpochTermination(ServerContext* context,
                                  const EpochBarrierPropagationNotification* request,
                                  EpochBarrierPropagationReply* reply) override;

    /**
     * @brief RPC Call to get a list of field nodes within a defined radius around a geographical location
     * @param context: the server context
     * @param request: that is sent from worker to the coordinator containing the center of the query area and the radius
     * @param reply: that is sent back from the coordinator to the worker containing the ids of all nodes in the defined area and their corresponding locations
     * @return success
     */
    Status GetNodesInRange(ServerContext*, const GetNodesInRangeRequest* request, GetNodesInRangeReply* reply) override;

    /**
     * @brief RPC Call to send errors to the coordinator
     * @param context: the server context
     * @param request: that is sent from worker to the coordinator and filled with information of errors
     * @param reply: that is sent back from the coordinator to the worker to confirm that notification was successful
     * @return success
     */
    Status SendErrors(ServerContext*, const SendErrorsMessage* request, ErrorReply* reply) override;

    /**
     * Request if soft stop can be performed for the query
     * @param context : the server context
     * @param request : that is sent from worker to the coordinator and containing the query id for which the soft stop to request
     * @param response : that is sent back from the coordinator to the worker if soft stop can be processed or not
     * @return true if soft stop can be performed else false
     */
    Status RequestSoftStop(::grpc::ServerContext* context,
                           const ::RequestSoftStopMessage* request,
                           ::StopRequestReply* response) override;

    /**
     * Notify coordinator that for a subquery plan the soft stop is triggered or not
     * @param context : the server context
     * @param request : that is sent from worker to the coordinator and containing the query id, sub query id, and if soft stop is triggered
     * @param response : that is sent back from the coordinator to the worker if request is processed
     * @return true if coordinator successfully recorded the information else false
     */
    Status notifySourceStopTriggered(::grpc::ServerContext* context,
                                     const ::SoftStopTriggeredMessage* request,
                                     ::SoftStopTriggeredReply* response) override;

    /**
     * Notify coordinator that for a subquery plan the soft stop is completed or not
     * @param context : the server context
     * @param request : that is sent from worker to the coordinator and containing the query id, sub query id, and if soft stop is completed
     * @param response : that is sent back from the coordinator to the worker if request is processed
     * @return true if the request is acknowledged
     */
    Status NotifySoftStopCompleted(::grpc::ServerContext* context,
                                   const ::SoftStopCompletionMessage* request,
                                   ::SoftStopCompletionReply* response) override;

    /**
     * @brief inform the coordinator that a mobile devices reconnect prediction has changed
     * @param request : sent from worker to coordinator containing the id of the mobile device and the scheduled reconnect
     * consisting of the id of the field node which the mobile device expects to connect to and the location and time at which
     * the reconnect is expected to happen
     * @param reply : sent from coordinator to worker not containing any data
     * @return OK if the coordinator succesfully recorded the data, CANCELLED otherwise
     */
    Status SendScheduledReconnect(ServerContext*,
                                  const SendScheduledReconnectRequest* request,
                                  SendScheduledReconnectReply* reply) override;

    /**
     * @brief inform the coordinator that the devices location has changed
     * @param request : sent from worker to coordinator containing the id of the mobile device, its new location and the time
     * when this location was recorded
     * @param reply : sent from coordinator to worker containing no data
     * @return OK in any case
     */
    Status SendLocationUpdate(ServerContext*, const LocationUpdateRequest* request, LocationUpdateReply* reply) override;

  private:
    QueryServicePtr queryService;
    TopologyManagerServicePtr topologyManagerService;
    SourceCatalogServicePtr sourceCatalogService;
    QueryCatalogServicePtr queryCatalogService;
    Monitoring::MonitoringManagerPtr monitoringManager;
    ReplicationServicePtr replicationService;
    NES::Spatial::Index::Experimental::LocationServicePtr locationService;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_GRPC_COORDINATORRPCSERVER_HPP_

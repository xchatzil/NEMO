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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <CoordinatorRPCService.pb.h>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Monitoring/Metrics/Gauge/RegistrationMetrics.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Experimental/NodeTypeUtilities.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <filesystem>
#include <fstream>
#include <health.grpc.pb.h>
#include <string>
namespace NES {

namespace detail {

/**
 * @brief Listener to process an Rpc Execution
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
class RpcExecutionListener {
  public:
    /**
     * @brief Executes the rpc call calling the right grpc method
     * @return grpc invocation status code
     */
    virtual grpc::Status rpcCall(const RequestType&, ReplyType*) = 0;

    /**
     * @brief called upon the successful completion of the rpc with a reply
     * @return value to be returned on success
     */
    virtual ReturnType onSuccess(const ReplyType&) = 0;

    /**
     * @brief called upon an error during the rpc
     * @return true if a retry must be attempted
     */
    virtual bool onPartialFailure(const grpc::Status&) = 0;

    /**
     * @brief called upon the unsuccessful completion of the rpc with a reply
     * @return value to be returned on fail
     */
    virtual ReturnType onFailure() = 0;
};

/**
 * @brief This methods perform an rpc by sending a request and waiting for a reply.
 * It may retry in the case of a failure.
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 * @param request the request to send
 * @param retryAttempts the number of retry attempts
 * @param backOffTime waiting time between retrial
 * @param listener the object that manages the rpc invocation
 * @return a return object in the case of success or failure
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
[[nodiscard]] ReturnType processRpc(const RequestType& request,
                                    uint32_t retryAttempts,
                                    std::chrono::milliseconds backOffTime,
                                    RpcExecutionListener<ReturnType, RequestType, ReplyType>& listener) {

    for (uint32_t i = 0; i < retryAttempts; ++i, backOffTime *= 2) {
        ReplyType reply;
        Status status = listener.rpcCall(request, &reply);

        if (status.ok()) {
            return listener.onSuccess(reply);
        } else if (listener.onPartialFailure(status)) {
            usleep(std::chrono::duration_cast<std::chrono::microseconds>(backOffTime).count());
        } else {
            break;
        }
    }
    return listener.onFailure();
}

/**
 * @brief This methods perform an rpc by sending a request and waiting for a reply.
 * It may retry in the case of a failure.
 * @tparam ReturnType the return type of the rpc
 * @tparam RequestType type of the object sent as request
 * @tparam ReplyType type of the object expected as reply
 * @param request the request to send
 * @param retryAttempts the number of retry attempts
 * @param backOffTime waiting time between retrial
 * @param func the call to the selected rpc
 * @return a return object in the case of success or failure
 */
template<typename ReturnType, typename RequestType, typename ReplyType>
[[nodiscard]] ReturnType processGenericRpc(const RequestType& request,
                                           uint32_t retryAttempts,
                                           std::chrono::milliseconds backOffTime,
                                           std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)>&& func) {
    class GenericRpcListener : public detail::RpcExecutionListener<bool, RequestType, ReplyType> {
      public:
        explicit GenericRpcListener(std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)>&& func)
            : func(std::move(func)) {}

        grpc::Status rpcCall(const RequestType& request, ReplyType* reply) override {
            ClientContext context;

            return func(&context, request, reply);
        }
        bool onSuccess(const ReplyType& reply) override {
            NES_DEBUG("CoordinatorRPCClient::: status ok return success=" << reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient:: error=" << status.error_code() << ": " << status.error_message());
            return false;
        }
        bool onFailure() override { return false; }

      private:
        std::function<grpc::Status(ClientContext*, const RequestType&, ReplyType*)> func;
    };

    auto listener = GenericRpcListener(std::move(func));
    return detail::processRpc(request, retryAttempts, backOffTime, listener);
}

}// namespace detail

CoordinatorRPCClient::CoordinatorRPCClient(const std::string& address,
                                           uint32_t rpcRetryAttemps,
                                           std::chrono::milliseconds rpcBackoff)
    : address(address), rpcRetryAttemps(rpcRetryAttemps), rpcBackoff(rpcBackoff) {
    NES_DEBUG("CoordinatorRPCClient(): creating channels to address =" << address);
    rpcChannel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    if (rpcChannel) {
        NES_DEBUG("CoordinatorRPCClient::connecting: channel successfully created");
    } else {
        NES_THROW_RUNTIME_ERROR("CoordinatorRPCClient::connecting error while creating channel");
    }
    coordinatorStub = CoordinatorRPCService::NewStub(rpcChannel);
    workerId = SIZE_MAX;
}

bool CoordinatorRPCClient::registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources) {
    NES_DEBUG("CoordinatorRPCClient::registerPhysicalSources: got "
              << physicalSources.size() << " physical sources to register for worker with id " << workerId);

    RegisterPhysicalSourcesRequest request;
    request.set_id(workerId);

    for (const auto& physicalSource : physicalSources) {
        PhysicalSourceDefinition* physicalSourceDefinition = request.add_physicalsources();
        physicalSourceDefinition->set_sourcetype(physicalSource->getPhysicalSourceType()->getSourceTypeAsString());
        physicalSourceDefinition->set_physicalsourcename(physicalSource->getPhysicalSourceName());
        physicalSourceDefinition->set_logicalsourcename(physicalSource->getLogicalSourceName());
    }

    NES_DEBUG("CoordinatorRPCClient::registerPhysicalSources request=" << request.DebugString());

    return detail::processGenericRpc<bool, RegisterPhysicalSourcesRequest, RegisterPhysicalSourcesReply>(
        request,
        rpcRetryAttemps,
        rpcBackoff,
        [this](ClientContext* context, const RegisterPhysicalSourcesRequest& request, RegisterPhysicalSourcesReply* reply) {
            return coordinatorStub->RegisterPhysicalSource(context, request, reply);
        });
}

bool CoordinatorRPCClient::registerLogicalSource(const std::string& logicalSourceName, const std::string& filePath) {
    NES_DEBUG("CoordinatorRPCClient: registerLogicalSource " << logicalSourceName << " with path" << filePath);

    // Check if file can be found on system and read.
    std::filesystem::path path{filePath.c_str()};
    if (!std::filesystem::exists(path) || !std::filesystem::is_regular_file(path)) {
        NES_ERROR("CoordinatorRPCClient: file does not exits");
        throw Exceptions::RuntimeException("files does not exist");
    }

    /* Read file from file system. */
    std::ifstream ifs(path.string().c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    RegisterLogicalSourceRequest request;
    request.set_id(workerId);
    request.set_logicalsourcename(logicalSourceName);
    request.set_sourceschema(fileContent);
    NES_DEBUG("CoordinatorRPCClient::RegisterLogicalSourceRequest request=" << request.DebugString());

    return detail::processGenericRpc<bool, RegisterLogicalSourceRequest, RegisterLogicalSourceReply>(
        request,
        rpcRetryAttemps,
        rpcBackoff,
        [this](ClientContext* context, const RegisterLogicalSourceRequest& request, RegisterLogicalSourceReply* reply) {
            return coordinatorStub->RegisterLogicalSource(context, request, reply);
        });
}

bool CoordinatorRPCClient::unregisterPhysicalSource(const std::string& logicalSourceName, const std::string& physicalSourceName) {
    NES_DEBUG("CoordinatorRPCClient: unregisterPhysicalSource physical source" << physicalSourceName << " from logical source ");

    UnregisterPhysicalSourceRequest request;
    request.set_id(workerId);
    request.set_physicalsourcename(physicalSourceName);
    request.set_logicalsourcename(logicalSourceName);
    NES_DEBUG("CoordinatorRPCClient::UnregisterPhysicalSourceRequest request=" << request.DebugString());

    return detail::processGenericRpc<bool, UnregisterPhysicalSourceRequest, UnregisterPhysicalSourceReply>(
        request,
        rpcRetryAttemps,
        rpcBackoff,
        [this](ClientContext* context, const UnregisterPhysicalSourceRequest& request, UnregisterPhysicalSourceReply* reply) {
            return coordinatorStub->UnregisterPhysicalSource(context, request, reply);
        });
}

bool CoordinatorRPCClient::unregisterLogicalSource(const std::string& logicalSourceName) {
    NES_DEBUG("CoordinatorRPCClient: unregisterLogicalSource source" << logicalSourceName);

    UnregisterLogicalSourceRequest request;
    request.set_id(workerId);
    request.set_logicalsourcename(logicalSourceName);
    NES_DEBUG("CoordinatorRPCClient::UnregisterLogicalSourceRequest request=" << request.DebugString());

    return detail::processGenericRpc<bool, UnregisterLogicalSourceRequest, UnregisterLogicalSourceReply>(
        request,
        rpcRetryAttemps,
        rpcBackoff,
        [this](ClientContext* context, const UnregisterLogicalSourceRequest& request, UnregisterLogicalSourceReply* reply) {
            return coordinatorStub->UnregisterLogicalSource(context, request, reply);
        });
}

bool CoordinatorRPCClient::addParent(uint64_t parentId) {
    NES_DEBUG("CoordinatorRPCClient: addParent parentId=" << parentId << " workerId=" << workerId);

    AddParentRequest request;
    request.set_parentid(parentId);
    request.set_childid(workerId);
    NES_DEBUG("CoordinatorRPCClient::AddParentRequest request=" << request.DebugString());

    return detail::processGenericRpc<bool, AddParentRequest, AddParentReply>(
        request,
        rpcRetryAttemps,
        rpcBackoff,
        [this](ClientContext* context, const AddParentRequest& request, AddParentReply* reply) {
            return coordinatorStub->AddParent(context, request, reply);
        });
}

bool CoordinatorRPCClient::replaceParent(uint64_t oldParentId, uint64_t newParentId) {
    NES_DEBUG("CoordinatorRPCClient: replaceParent oldParentId=" << oldParentId << " newParentId=" << newParentId
                                                                 << " workerId=" << workerId);

    ReplaceParentRequest request;
    request.set_childid(workerId);
    request.set_oldparent(oldParentId);
    request.set_newparent(newParentId);
    NES_DEBUG("CoordinatorRPCClient::replaceParent request=" << request.DebugString());

    class ReplaceParentListener : public detail::RpcExecutionListener<bool, ReplaceParentRequest, ReplaceParentReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub;

        explicit ReplaceParentListener(std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        Status rpcCall(const ReplaceParentRequest& request, ReplaceParentReply* reply) override {
            ClientContext context;

            return coordinatorStub->ReplaceParent(&context, request, reply);
        }
        bool onSuccess(const ReplaceParentReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::removeParent: status ok return success=" << reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::removeParent error=" << status.error_code() << ": " << status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = ReplaceParentListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttemps, rpcBackoff, listener);
}

uint64_t CoordinatorRPCClient::getId() const { return workerId; }

bool CoordinatorRPCClient::removeParent(uint64_t parentId) {
    NES_DEBUG("CoordinatorRPCClient: removeParent parentId" << parentId << " workerId=" << workerId);

    RemoveParentRequest request;
    request.set_parentid(parentId);
    request.set_childid(workerId);
    NES_DEBUG("CoordinatorRPCClient::RemoveParentRequest request=" << request.DebugString());

    class RemoveParentListener : public detail::RpcExecutionListener<bool, RemoveParentRequest, RemoveParentReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub;

        explicit RemoveParentListener(std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        Status rpcCall(const RemoveParentRequest& request, RemoveParentReply* reply) override {
            ClientContext context;

            return coordinatorStub->RemoveParent(&context, request, reply);
        }
        bool onSuccess(const RemoveParentReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::removeParent: status ok return success=" << reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::removeParent error=" << status.error_code() << ": " << status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = RemoveParentListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttemps, rpcBackoff, listener);
}

bool CoordinatorRPCClient::unregisterNode() {
    NES_DEBUG("CoordinatorRPCClient::unregisterNode workerId=" << workerId);

    UnregisterNodeRequest request;
    request.set_id(workerId);
    NES_DEBUG("CoordinatorRPCClient::unregisterNode request=" << request.DebugString());

    class UnRegisterNodeListener : public detail::RpcExecutionListener<bool, UnregisterNodeRequest, UnregisterNodeReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub;

        explicit UnRegisterNodeListener(std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        Status rpcCall(const UnregisterNodeRequest& request, UnregisterNodeReply* reply) override {
            ClientContext context;

            return coordinatorStub->UnregisterNode(&context, request, reply);
        }
        bool onSuccess(const UnregisterNodeReply& reply) override {
            NES_DEBUG("CoordinatorRPCClient::unregisterNode: status ok return success=" << reply.success());
            return reply.success();
        }
        bool onPartialFailure(const Status& status) override {
            NES_DEBUG(" CoordinatorRPCClient::unregisterNode error=" << status.error_code() << ": " << status.error_message());
            return false;
        }
        bool onFailure() override { return false; }
    };

    auto listener = UnRegisterNodeListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttemps, rpcBackoff, listener);
}

bool CoordinatorRPCClient::registerNode(const std::string& ipAddress,
                                        int64_t grpcPort,
                                        int64_t dataPort,
                                        int16_t numberOfSlots,
                                        const Monitoring::RegistrationMetrics& registrationMetrics,
                                        Spatial::Index::Experimental::Location fixedCoordinates,
                                        Spatial::Index::Experimental::NodeType spatialType,
                                        bool isTfInstalled) {

    RegisterNodeRequest request;
    request.set_address(ipAddress);
    request.set_grpcport(grpcPort);
    request.set_dataport(dataPort);
    request.set_numberofslots(numberOfSlots);
    request.set_spatialtype(Spatial::Util::NodeTypeUtilities::toProtobufEnum(spatialType));
    request.mutable_registrationmetrics()->Swap(registrationMetrics.serialize().get());
    request.set_istfinstalled(isTfInstalled);
    NES_TRACE("CoordinatorRPCClient::RegisterNodeRequest request=" << request.DebugString());
    Coordinates* pCoordinates = request.mutable_coordinates();
    pCoordinates->set_lat(fixedCoordinates.getLatitude());
    pCoordinates->set_lng(fixedCoordinates.getLongitude());

    class RegisterNodeListener : public detail::RpcExecutionListener<bool, RegisterNodeRequest, RegisterNodeReply> {
      public:
        uint64_t& workerId;
        std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub;

        explicit RegisterNodeListener(uint64_t& workerId, std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub)
            : workerId(workerId), coordinatorStub(coordinatorStub) {}

        Status rpcCall(const RegisterNodeRequest& request, RegisterNodeReply* reply) override {
            ClientContext context;

            return coordinatorStub->RegisterNode(&context, request, reply);
        }
        bool onSuccess(const RegisterNodeReply& reply) override {
            workerId = reply.id();
            return true;
        }
        bool onPartialFailure(const Status& status) override {
            NES_ERROR(" CoordinatorRPCClient::registerNode error=" << status.error_code() << ": " << status.error_message());
            switch (status.error_code()) {
                case grpc::UNIMPLEMENTED:
                case grpc::UNAVAILABLE: {
                    return true;// partial failure ok to continue
                }
                default: {
                    return false;// partial failure not ok to continue
                }
            }
        }
        bool onFailure() override { return false; }
    };

    auto listener = RegisterNodeListener{workerId, coordinatorStub};

    return detail::processRpc(request, rpcRetryAttemps, rpcBackoff, listener);
}

bool CoordinatorRPCClient::notifyQueryFailure(uint64_t queryId,
                                              uint64_t subQueryId,
                                              uint64_t workerId,
                                              uint64_t operatorId,
                                              std::string errorMsg) {

    // create & fill the protobuf
    QueryFailureNotification request;
    request.set_queryid(queryId);
    request.set_subqueryid(subQueryId);
    request.set_workerid(workerId);
    request.set_operatorid(operatorId);
    request.set_errormsg(errorMsg);

    class NotifyQueryFailureListener
        : public detail::RpcExecutionListener<bool, QueryFailureNotification, QueryFailureNotificationReply> {
      public:
        std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub;

        explicit NotifyQueryFailureListener(std::unique_ptr<CoordinatorRPCService::Stub>& coordinatorStub)
            : coordinatorStub(coordinatorStub) {}

        Status rpcCall(const QueryFailureNotification& request, QueryFailureNotificationReply* reply) override {
            ClientContext context;

            return coordinatorStub->NotifyQueryFailure(&context, request, reply);
        }
        bool onSuccess(const QueryFailureNotificationReply&) override {
            NES_DEBUG("WorkerRPCClient::NotifyQueryFailure: status ok");
            return true;
        }
        bool onPartialFailure(const Status&) override { return true; }
        bool onFailure() override { return false; }
    };

    auto listener = NotifyQueryFailureListener{coordinatorStub};

    return detail::processRpc(request, rpcRetryAttemps, rpcBackoff, listener);
}

std::vector<std::pair<uint64_t, Spatial::Index::Experimental::Location>>
CoordinatorRPCClient::getNodeIdsInRange(Spatial::Index::Experimental::LocationPtr location, double radius) {
    if (!location) {
        return {};
    }
    GetNodesInRangeRequest request;
    Coordinates* pCoordinates = request.mutable_coord();
    pCoordinates->set_lat(location->getLatitude());
    pCoordinates->set_lng(location->getLongitude());
    request.set_radius(radius);
    GetNodesInRangeReply reply;
    ClientContext context;

    Status status = coordinatorStub->GetNodesInRange(&context, request, &reply);

    std::vector<std::pair<uint64_t, Spatial::Index::Experimental::Location>> nodesInRange;
    for (WorkerLocationInfo workerInfo : *reply.mutable_nodes()) {
        nodesInRange.emplace_back(workerInfo.id(), workerInfo.coord());
    }
    return nodesInRange;
}

bool CoordinatorRPCClient::checkCoordinatorHealth(std::string healthServiceName) {
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<grpc::health::v1::Health::Stub> workerStub = grpc::health::v1::Health::NewStub(chan);

    grpc::health::v1::HealthCheckRequest request;
    request.set_service(healthServiceName);
    grpc::health::v1::HealthCheckResponse response;
    ClientContext context;
    Status status = workerStub->Check(&context, request, &response);

    if (status.ok()) {
        NES_TRACE("CoordinatorRPCClient::checkHealth: status ok return success=" << response.status());
        return response.status();
    } else {
        NES_ERROR(" CoordinatorRPCClient::checkHealth error=" << status.error_code() << ": " << status.error_message());
        return response.status();
    }
}

bool CoordinatorRPCClient::notifyEpochTermination(uint64_t timestamp, uint64_t querySubPlanId) {
    EpochBarrierPropagationNotification request;
    request.set_timestamp(timestamp);
    request.set_queryid(querySubPlanId);
    EpochBarrierPropagationReply reply;
    ClientContext context;
    Status status = coordinatorStub->NotifyEpochTermination(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::PropagatePunctuation: status ok");
        return true;
    }
    return false;
}

bool CoordinatorRPCClient::sendErrors(uint64_t workerId, std::string errorMsg) {

    // create & fill the protobuf
    SendErrorsMessage request;
    request.set_workerid(workerId);
    request.set_errormsg(errorMsg);

    ErrorReply reply;

    ClientContext context;

    Status status = coordinatorStub->SendErrors(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::SendErrors: status ok");
        return true;
    }
    return false;
}

bool CoordinatorRPCClient::checkAndMarkForSoftStop(QueryId queryId, QuerySubPlanId subPlanId, OperatorId sourceId) {

    //Build request
    RequestSoftStopMessage requestSoftStopMessage;
    requestSoftStopMessage.set_queryid(queryId);
    requestSoftStopMessage.set_subqueryid(subPlanId);
    requestSoftStopMessage.set_sourceid(sourceId);

    //Build response
    StopRequestReply stopRequestReply;

    ClientContext context;
    coordinatorStub->RequestSoftStop(&context, requestSoftStopMessage, &stopRequestReply);

    //return the response
    return stopRequestReply.success();
}

bool CoordinatorRPCClient::notifySourceStopTriggered(QueryId queryId,
                                                     QuerySubPlanId querySubPlanId,
                                                     OperatorId sourceId,
                                                     Runtime::QueryTerminationType queryTermination) {
    NES_ASSERT2_FMT(queryTermination == Runtime::QueryTerminationType::Graceful, "Wrong termination requested");

    //Build request
    SoftStopTriggeredMessage softStopTriggeredMessage;
    softStopTriggeredMessage.set_queryid(queryId);
    softStopTriggeredMessage.set_querysubplanid(querySubPlanId);
    softStopTriggeredMessage.set_sourceid(sourceId);

    //Build response
    SoftStopTriggeredReply softStopTriggeredReply;

    ClientContext context;
    coordinatorStub->notifySourceStopTriggered(&context, softStopTriggeredMessage, &softStopTriggeredReply);

    //return the response
    return softStopTriggeredReply.success();
}

bool CoordinatorRPCClient::notifySoftStopCompleted(QueryId queryId, QuerySubPlanId querySubPlanId) {
    //Build request
    SoftStopCompletionMessage softStopCompletionMessage;
    softStopCompletionMessage.set_queryid(queryId);
    softStopCompletionMessage.set_querysubplanid(querySubPlanId);

    //Build response
    SoftStopCompletionReply softStopCompletionReply;

    ClientContext context;
    coordinatorStub->NotifySoftStopCompleted(&context, softStopCompletionMessage, &softStopCompletionReply);

    //return the response
    return softStopCompletionReply.success();
}

bool CoordinatorRPCClient::sendReconnectPrediction(uint64_t nodeId,
                                                   Spatial::Mobility::Experimental::ReconnectPrediction scheduledReconnect) {
    ClientContext context;
    SendScheduledReconnectRequest request;
    SendScheduledReconnectReply reply;

    request.set_deviceid(nodeId);
    SerializableReconnectPrediction* reconnectPoint = request.mutable_reconnect();
    reconnectPoint->set_id(scheduledReconnect.expectedNewParentId);
    reconnectPoint->set_time(scheduledReconnect.expectedTime);

    coordinatorStub->SendScheduledReconnect(&context, request, &reply);
    return reply.success();
}

bool CoordinatorRPCClient::sendLocationUpdate(uint64_t nodeId, Spatial::Index::Experimental::WaypointPtr locationUpdate) {
    ClientContext context;
    LocationUpdateRequest request;
    LocationUpdateReply reply;

    request.set_id(nodeId);

    Coordinates* coordinates = request.mutable_coord();
    coordinates->set_lat(locationUpdate->getLocation()->getLatitude());
    coordinates->set_lng(locationUpdate->getLocation()->getLongitude());

    if (locationUpdate->getTimestamp()) {
        request.set_time(locationUpdate->getTimestamp().value());
    }
    coordinatorStub->SendLocationUpdate(&context, request, &reply);
    return reply.success();
}
}// namespace NES
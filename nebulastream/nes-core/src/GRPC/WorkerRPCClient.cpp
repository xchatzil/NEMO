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

#include <API/Schema.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Util/Logger/Logger.hpp>
#include <health.grpc.pb.h>

namespace NES {

bool WorkerRPCClient::registerQuery(const std::string& address, const QueryPlanPtr& queryPlan) {
    QueryId queryId = queryPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_DEBUG("WorkerRPCClient::registerQuery address=" << address << " queryId=" << queryId
                                                        << " querySubPlanId = " << querySubPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;

    // serialize query plan.
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> " << request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::registerQuery: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::registerQuery "
              "error="
              << status.error_code() << ": " << status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::registerQuery");
}

bool WorkerRPCClient::registerQueryAsync(const std::string& address,
                                         const QueryPlanPtr& queryPlan,
                                         const CompletionQueuePtr& cq) {
    QueryId queryId = queryPlan->getQueryId();
    QuerySubPlanId querySubPlanId = queryPlan->getQuerySubPlanId();
    NES_DEBUG("WorkerRPCClient::registerQueryAsync address=" << address << " queryId=" << queryId
                                                             << " querySubPlanId = " << querySubPlanId);

    // wrap the query id and the query operators in the protobuf register query request object.
    RegisterQueryRequest request;
    // serialize query plan.
    auto serializableQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializableQueryPlan);

    NES_TRACE("WorkerRPCClient:registerQuery -> " << request.DebugString());
    RegisterQueryReply reply;
    ClientContext context;

    grpc::ChannelArguments args;
    args.SetInt("test_key", querySubPlanId);
    std::shared_ptr<::grpc::Channel> channel = grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args);
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(channel);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<RegisterQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncRegisterQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::checkAsyncResult(const std::map<CompletionQueuePtr, uint64_t>& queues, RpcClientModes mode) {
    NES_DEBUG("start checkAsyncResult for mode=" << mode << " for " << queues.size() << " queues");
    bool result = true;
    for (const auto& queue : queues) {
        //wait for all deploys to come back
        void* got_tag = nullptr;
        bool ok = false;
        uint64_t cnt = 0;
        // Block until the next result is available in the completion queue "completionQueue".
        while (cnt != queue.second && queue.first->Next(&got_tag, &ok)) {
            // The tag in this example is the memory location of the call object
            bool status = false;
            if (mode == Register) {
                auto* call = static_cast<AsyncClientCall<RegisterQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Unregister) {
                auto* call = static_cast<AsyncClientCall<UnregisterQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Start) {
                auto* call = static_cast<AsyncClientCall<StartQueryReply>*>(got_tag);
                status = call->status.ok();
                delete call;
            } else if (mode == Stop) {
                auto* call = static_cast<AsyncClientCall<StopQueryReply>*>(got_tag);
                status = call->status.ok();
                if (!status) {
                    NES_ERROR("RPC Failed: " << call->status.error_message());
                }
                delete call;
            } else {
                NES_NOT_IMPLEMENTED();
            }

            if (!status) {
                NES_THROW_RUNTIME_ERROR("RPC failed, a scheduled async call for mode" << mode << " failed");
            }

            // Once we're complete, deallocate the call object.
            cnt++;
        }
    }
    NES_DEBUG("checkAsyncResult for mode=" << mode << " succeed");
    return result;
}
bool WorkerRPCClient::unregisterQueryAsync(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::unregisterQueryAsync address=" << address << " queryId=" << queryId);

    UnregisterQueryRequest request;
    request.set_queryid(queryId);

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<UnregisterQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncUnregisterQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::unregisterQuery(const std::string& address, QueryId queryId) {
    NES_DEBUG("WorkerRPCClient::unregisterQuery address=" << address << " queryId=" << queryId);

    UnregisterQueryRequest request;
    request.set_queryid(queryId);

    UnregisterQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UnregisterQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::unregisterQuery: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::unregisterQuery "
              "error="
              << status.error_code() << ": " << status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::unregisterQuery");
}

bool WorkerRPCClient::startQuery(const std::string& address, QueryId queryId) {
    NES_DEBUG("WorkerRPCClient::startQuery address=" << address << " queryId=" << queryId);

    StartQueryRequest request;
    request.set_queryid(queryId);

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    Status status = workerStub->StartQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::startQuery: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_DEBUG(" WorkerRPCClient::startQuery "
              "error="
              << status.error_code() << ": " << status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::startQuery");
}

bool WorkerRPCClient::startQueryAsyn(const std::string& address, QueryId queryId, const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::startQueryAsync address=" << address << " queryId=" << queryId);

    StartQueryRequest request;
    request.set_queryid(queryId);

    StartQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StartQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStartQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::stopQuery(const std::string& address, QueryId queryId, Runtime::QueryTerminationType terminationType) {
    NES_DEBUG("WorkerRPCClient::markQueryForStop address=" << address << " queryId=" << queryId);

    StopQueryRequest request;
    request.set_queryid(queryId);
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->StopQuery(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::markQueryForStop: status ok return success=" << reply.success());
        return reply.success();
    }
    NES_ERROR(" WorkerRPCClient::markQueryForStop "
              "error="
              << status.error_code() << ": " << status.error_message());
    throw Exceptions::RuntimeException("Error while WorkerRPCClient::markQueryForStop");
}

bool WorkerRPCClient::stopQueryAsync(const std::string& address,
                                     QueryId queryId,
                                     Runtime::QueryTerminationType terminationType,
                                     const CompletionQueuePtr& cq) {
    NES_DEBUG("WorkerRPCClient::stopQueryAsync address=" << address << " queryId=" << queryId);

    StopQueryRequest request;
    request.set_queryid(queryId);
    request.set_queryterminationtype(static_cast<uint64_t>(terminationType));

    StopQueryReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);

    // Call object to store rpc data
    auto* call = new AsyncClientCall<StopQueryReply>;

    // workerStub->PrepareAsyncRegisterQuery() creates an RPC object, returning
    // an instance to store in "call" but does not actually start the RPC
    // Because we are using the asynchronous API, we need to hold on to
    // the "call" instance in order to get updates on the ongoing RPC.
    call->responseReader = workerStub->PrepareAsyncStopQuery(&call->context, request, cq.get());

    // StartCall initiates the RPC call
    call->responseReader->StartCall();

    // Request that, upon completion of the RPC, "reply" be updated with the
    // server's response; "status" with the indication of whether the operation
    // was successful. Tag the request with the memory address of the call object.
    call->responseReader->Finish(&call->reply, &call->status, (void*) call);

    return true;
}

bool WorkerRPCClient::registerMonitoringPlan(const std::string& address, const Monitoring::MonitoringPlanPtr& plan) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address=" << address);

    MonitoringRegistrationRequest request;
    for (auto metric : plan->getMetricTypes()) {
        request.mutable_metrictypes()->Add(metric);
    }
    ClientContext context;
    MonitoringRegistrationReply reply;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->RegisterMonitoringPlan(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: status ok");
        return true;
    }
    NES_THROW_RUNTIME_ERROR(" WorkerRPCClient::RequestMonitoringData error=" + std::to_string(status.error_code()) + ": "
                            + status.error_message());
    return false;
}

std::string WorkerRPCClient::requestMonitoringData(const std::string& address) {
    NES_DEBUG("WorkerRPCClient: Monitoring request address=" << address);
    MonitoringDataRequest request;
    ClientContext context;
    MonitoringDataReply reply;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetMonitoringData(&context, request, &reply);

    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::RequestMonitoringData: metrics received " << reply.metricsasjson());
        return reply.metricsasjson();
    }
    NES_THROW_RUNTIME_ERROR("WorkerRPCClient::RequestMonitoringData error=" << std::to_string(status.error_code()) << ": "
                                                                            << status.error_message());
}

bool WorkerRPCClient::injectEpochBarrier(uint64_t timestamp, uint64_t queryId, const std::string& address) {
    EpochBarrierNotification request;
    request.set_timestamp(timestamp);
    request.set_queryid(queryId);
    EpochBarrierReply reply;
    ClientContext context;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->InjectEpochBarrier(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::PropagatePunctuation: status ok");
        return true;
    }
    return false;
}

bool WorkerRPCClient::bufferData(const std::string& address, uint64_t querySubPlanId, uint64_t uniqueNetworkSinDescriptorId) {
    NES_DEBUG("WorkerRPCClient::buffering Data on address=" << address);
    BufferRequest request;
    request.set_querysubplanid(querySubPlanId);
    request.set_uniquenetworksinkdescriptorid(uniqueNetworkSinDescriptorId);
    BufferReply reply;
    ClientContext context;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->BeginBuffer(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::BeginBuffer: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_ERROR(" WorkerRPCClient::BeginBuffer "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exceptions::RuntimeException("Error while WorkerRPCClient::markQueryForStop");
    }
    return false;
}

bool WorkerRPCClient::updateNetworkSink(const std::string& address,
                                        uint64_t newNodeId,
                                        const std::string& newHostname,
                                        uint32_t newPort,
                                        uint64_t querySubPlanId,
                                        uint64_t uniqueNetworkSinDescriptorId) {
    UpdateNetworkSinkRequest request;
    request.set_newnodeid(newNodeId);
    request.set_newhostname(newHostname);
    request.set_newport(newPort);
    request.set_querysubplanid(querySubPlanId);
    request.set_uniquenetworksinkdescriptorid(uniqueNetworkSinDescriptorId);

    UpdateNetworkSinkReply reply;
    ClientContext context;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->UpdateNetworkSink(&context, request, &reply);
    if (status.ok()) {
        NES_DEBUG("WorkerRPCClient::UpdateNetworkSinks: status ok return success=" << reply.success());
        return reply.success();
    } else {
        NES_ERROR(" WorkerRPCClient::UpdateNetworkSinks "
                  "error="
                  << status.error_code() << ": " << status.error_message());
        throw Exceptions::RuntimeException("Error while WorkerRPCClient::updateNetworkSinks");
    }
}

bool WorkerRPCClient::checkHealth(const std::string& address, std::string healthServiceName) {
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    std::unique_ptr<grpc::health::v1::Health::Stub> workerStub = grpc::health::v1::Health::NewStub(chan);

    grpc::health::v1::HealthCheckRequest request;
    request.set_service(healthServiceName);
    grpc::health::v1::HealthCheckResponse response;
    ClientContext context;
    Status status = workerStub->Check(&context, request, &response);

    if (status.ok()) {
        NES_TRACE("WorkerRPCClient::checkHealth: status ok return success=" << response.status());
        return response.status();
    } else {
        NES_ERROR(" WorkerRPCClient::checkHealth error=" << status.error_code() << ": " << status.error_message());
        return response.status();
    }
}

Spatial::Index::Experimental::WaypointPtr WorkerRPCClient::getWaypoint(const std::string& address) {
    NES_DEBUG("WorkerRPCClient: Requesting location from " << address)
    ClientContext context;
    GetLocationRequest request;
    GetLocationReply reply;
    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetLocation(&context, request, &reply);
    if (reply.has_coord()) {
        auto coord = reply.coord();
        auto timestamp = reply.timestamp();
        //if timestamp is valid, include it in waypoint
        if (timestamp != 0) {
            return std::make_shared<Spatial::Index::Experimental::Waypoint>(
                Spatial::Index::Experimental::Location(coord.lat(), coord.lng()),
                timestamp);
        }
        //no valid timestamp to include
        return std::make_shared<Spatial::Index::Experimental::Waypoint>(
            Spatial::Index::Experimental::Location(coord.lat(), coord.lng()));
    }
    //location is invalid
    return std::make_shared<Spatial::Index::Experimental::Waypoint>(Spatial::Index::Experimental::Waypoint::invalid());
}

NES::Spatial::Mobility::Experimental::ReconnectSchedulePtr WorkerRPCClient::getReconnectSchedule(const std::string& address) {
    NES_DEBUG("WorkerRPCClient: requesting reconnect schedule from" << address)
    ClientContext context;
    GetReconnectScheduleRequest request;
    GetReconnectScheduleReply reply;

    std::shared_ptr<::grpc::Channel> chan = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());

    std::unique_ptr<WorkerRPCService::Stub> workerStub = WorkerRPCService::NewStub(chan);
    Status status = workerStub->GetReconnectSchedule(&context, request, &reply);

    //check if the reply contained a schedule
    if (reply.has_schedule()) {
        const auto& schedule = reply.schedule();
        //get start and enpoint of the predicted trajectory line
        auto start = std::make_shared<Spatial::Index::Experimental::Location>(schedule.pathstart());
        auto end = std::make_shared<Spatial::Index::Experimental::Location>(schedule.pathend());

        //get the position of the device when at the moment of the last node index update
        auto lastUpdatePosition = std::make_shared<Spatial::Index::Experimental::Location>(schedule.lastindexupdateposition());

        //iterate of the vector of reconnects and get all planned reconnects
        auto vec = std::make_shared<std::vector<std::shared_ptr<Spatial::Mobility::Experimental::ReconnectPoint>>>();
        for (int i = 0; i < schedule.reconnectpoints_size(); ++i) {
            const auto& reconnectData = schedule.reconnectpoints(i);
            auto loc = NES::Spatial::Index::Experimental::Location(reconnectData.coord().lat(), reconnectData.coord().lng());
            vec->push_back(std::make_shared<NES::Spatial::Mobility::Experimental::ReconnectPoint>(
                Spatial::Mobility::Experimental::ReconnectPoint{
                    loc,
                    NES::Spatial::Mobility::Experimental::ReconnectPrediction{reconnectData.reconnectprediction().id(),
                                                                              reconnectData.reconnectprediction().time()}}));
        }

        //construct a schedule from the received data
        return std::make_shared<NES::Spatial::Mobility::Experimental::ReconnectSchedule>(reply.schedule().parentid(),
                                                                                         start,
                                                                                         end,
                                                                                         lastUpdatePosition,
                                                                                         vec);
    }
    //if no schedule was received, return an empty schedule
    return std::make_shared<Spatial::Mobility::Experimental::ReconnectSchedule>(
        Spatial::Mobility::Experimental::ReconnectSchedule::Empty());
}

}// namespace NES

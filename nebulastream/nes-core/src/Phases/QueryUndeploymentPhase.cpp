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

#include <Exceptions/QueryUndeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Phases/QueryUndeploymentPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>

namespace NES {

QueryUndeploymentPhase::QueryUndeploymentPhase(TopologyPtr topology,
                                               GlobalExecutionPlanPtr globalExecutionPlan,
                                               WorkerRPCClientPtr workerRpcClient)
    : topology(std::move(topology)), globalExecutionPlan(std::move(globalExecutionPlan)),
      workerRPCClient(std::move(workerRpcClient)) {
    NES_DEBUG("QueryUndeploymentPhase()");
}

QueryUndeploymentPhasePtr QueryUndeploymentPhase::create(TopologyPtr topology,
                                                         GlobalExecutionPlanPtr globalExecutionPlan,
                                                         WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryUndeploymentPhase>(
        QueryUndeploymentPhase(std::move(topology), std::move(globalExecutionPlan), std::move(workerRpcClient)));
}

bool QueryUndeploymentPhase::execute(const QueryId queryId, SharedQueryPlanStatus::Value sharedQueryPlanStatus) {
    NES_DEBUG("QueryUndeploymentPhase::stopAndUndeployQuery : queryId=" << queryId);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    if (executionNodes.empty()) {
        NES_ERROR("QueryUndeploymentPhase: Unable to find ExecutionNodes where the query " << queryId << " is deployed");
        return false;
    }

    NES_DEBUG("QueryUndeploymentPhase:removeQuery: stop query");
    bool successStop = stopQuery(queryId, executionNodes, sharedQueryPlanStatus);
    if (successStop) {
        NES_DEBUG("QueryUndeploymentPhase:removeQuery: stop query successful for " << queryId);
    } else {
        NES_ERROR("QueryUndeploymentPhase:removeQuery: stop query failed for " << queryId);
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw QueryUndeploymentException("Failed to stop the query " + std::to_string(queryId) + '.');
    }

    NES_DEBUG("QueryUndeploymentPhase:removeQuery: undeploy query " << queryId);
    bool successUndeploy = undeployQuery(queryId, executionNodes);
    if (successUndeploy) {
        NES_DEBUG("QueryUndeploymentPhase:removeQuery: undeploy query successful");
    } else {
        NES_ERROR("QueryUndeploymentPhase:removeQuery: undeploy query failed");
        // XXX: C++2a: Modernize to std::format("Failed to stop the query {}.", queryId)
        throw QueryUndeploymentException("Failed to stop the query " + std::to_string(queryId) + '.');
    }

    const std::map<uint64_t, uint32_t>& resourceMap = globalExecutionPlan->getMapOfTopologyNodeIdToOccupiedResource(queryId);

    for (auto entry : resourceMap) {
        NES_TRACE("QueryUndeploymentPhase: Releasing " << entry.second << " resources for the node " << entry.first);
        topology->increaseResources(entry.first, entry.second);
    }

    return globalExecutionPlan->removeQuerySubPlans(queryId);
}

bool QueryUndeploymentPhase::stopQuery(QueryId queryId,
                                       const std::vector<ExecutionNodePtr>& executionNodes,
                                       SharedQueryPlanStatus::Value sharedQueryPlanStatus) {
    NES_DEBUG("QueryUndeploymentPhase:markQueryForStop queryId=" << queryId);
    //NOTE: the uncommented lines below have to be activated for async calls
    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (auto&& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryUndeploymentPhase::markQueryForStop at execution node with id=" << executionNode->getId()
                                                                                        << " and IP=" << rpcAddress);

        Runtime::QueryTerminationType queryTerminationType;

        if (SharedQueryPlanStatus::Updated == sharedQueryPlanStatus || SharedQueryPlanStatus::Stopped == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::HardStop;
        } else if (SharedQueryPlanStatus::Failed == sharedQueryPlanStatus) {
            queryTerminationType = Runtime::QueryTerminationType::Failure;
        } else {
            NES_ERROR("Unhandled request type " << SharedQueryPlanStatus::toString(sharedQueryPlanStatus));
            NES_NOT_IMPLEMENTED();
        }

        bool success = workerRPCClient->stopQueryAsync(rpcAddress, queryId, queryTerminationType, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryUndeploymentPhase::markQueryForStop " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryUndeploymentPhase::markQueryForStop " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }

    // activate below for async calls
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Stop);
    NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id " << queryId << " success=" << result);
    return true;
}

bool QueryUndeploymentPhase::undeployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes) {
    NES_DEBUG("QueryUndeploymentPhase::undeployQuery queryId=" << queryId);

    std::map<CompletionQueuePtr, uint64_t> completionQueues;

    for (const ExecutionNodePtr& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryUndeploymentPhase::undeployQuery query at execution node with id=" << executionNode->getId()
                                                                                           << " and IP=" << rpcAddress);
        //        bool success = workerRPCClient->unregisterQuery(rpcAddress, queryId);
        bool success = workerRPCClient->unregisterQueryAsync(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryUndeploymentPhase::undeployQuery query " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryUndeploymentPhase::undeployQuery " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }

        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Unregister);
    NES_DEBUG("QueryDeploymentPhase: Finished stopping execution plan for query with Id " << queryId << " success=" << result);
    return result;
}
}// namespace NES
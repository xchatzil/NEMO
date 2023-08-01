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

#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ThreadNaming.hpp>

namespace NES {

CoordinatorHealthCheckService::CoordinatorHealthCheckService(TopologyManagerServicePtr topologyManagerService,
                                                             WorkerRPCClientPtr workerRPCClient,
                                                             std::string healthServiceName,
                                                             Configurations::CoordinatorConfigurationPtr coordinatorConfiguration)
    : topologyManagerService(topologyManagerService), workerRPCClient(workerRPCClient),
      coordinatorConfiguration(coordinatorConfiguration) {
    id = 9999;
    this->healthServiceName = healthServiceName;
}

void CoordinatorHealthCheckService::startHealthCheck() {
    NES_DEBUG("CoordinatorHealthCheckService::startHealthCheck");
    isRunning = true;
    NES_DEBUG("start health checking on coordinator");
    healthCheckingThread = std::make_shared<std::thread>(([this]() {
        setThreadName("nesHealth");
        auto waitTime = std::chrono::seconds(this->coordinatorConfiguration->coordinatorHealthCheckWaitTime.getValue());
        while (isRunning) {
            for (auto node : nodeIdToTopologyNodeMap.lock_table()) {
                auto nodeIp = node.second->getIpAddress();
                auto nodeGrpcPort = node.second->getGrpcPort();
                std::string destAddress = nodeIp + ":" + std::to_string(nodeGrpcPort);

                //check health
                NES_TRACE("NesCoordinator::healthCheck: checking node=" << destAddress);
                auto res = workerRPCClient->checkHealth(destAddress, healthServiceName);
                if (res) {
                    NES_TRACE("NesCoordinator::healthCheck: node=" << destAddress << " is alive");
                } else {
                    NES_WARNING("NesCoordinator::healthCheck: node=" << destAddress << " went dead so we remove it");
                    if (topologyManagerService->getRootNode()->getId() == node.second->getId()) {
                        NES_WARNING("The failing node is the root node so we cannot delete it");
                        shutdownRPC->set_value(true);
                        return;
                    } else {
                        auto ret = topologyManagerService->removePhysicalNode(node.second);
                        if (ret) {
                            NES_TRACE("NesCoordinator::healthCheck: remove node =" << destAddress << " successfully");
                        } else {
                            NES_WARNING("Node went offline but could not be removed from topology");
                        }
                        //TODO Create issue for remove and child
                        //TODO add remove from source catalog
                    }
                }
            }
            {
                std::unique_lock<std::mutex> lk(cvMutex);
                cv.wait_for(lk, waitTime, [this] {
                    return isRunning == false;
                });
            }
        }
        shutdownRPC->set_value(true);
        NES_DEBUG("NesCoordinator: stop health checking");
    }));
}

}// namespace NES
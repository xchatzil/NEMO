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

#ifndef NES_CORE_INCLUDE_SERVICES_ABSTRACTHEALTHCHECKSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_ABSTRACTHEALTHCHECKSERVICE_HPP_

#include <Util/libcuckoo/cuckoohash_map.hh>
#include <future>
#include <map>
#include <memory>
#include <stdint.h>
#include <thread>

namespace NES {

class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class TopologyManagerService;
using TopologyManagerServicePtr = std::shared_ptr<TopologyManagerService>;

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Configurations {

class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;

}// namespace Configurations

/**
 * @brief: This class is responsible for handling requests related to monitor the alive status of nodes.
 */
class AbstractHealthCheckService {
  public:
    AbstractHealthCheckService();

    virtual ~AbstractHealthCheckService(){};

    /**
     * Method to start the health checking
     */
    virtual void startHealthCheck() = 0;

    /**
     * Method to stop the health checking
     */
    void stopHealthCheck();

    /**
     * Method to add a node for health checking
     * @param node pointer to the node in the topology
     */
    void addNodeToHealthCheck(TopologyNodePtr node);

    /**
     * Method to remove a node from the health checking
     * @param node pointer to the node in the topology
     */
    void removeNodeFromHealthCheck(TopologyNodePtr node);

    /**
     * Method to return if the health server is still running
     * @return
     */
    bool getRunning();

  protected:
    std::shared_ptr<std::thread> healthCheckingThread;
    std::atomic<bool> isRunning = false;
    std::shared_ptr<std::promise<bool>> shutdownRPC = std::make_shared<std::promise<bool>>();
    cuckoohash_map<uint64_t, TopologyNodePtr> nodeIdToTopologyNodeMap;
    uint64_t id;
    std::string healthServiceName;
    std::condition_variable cv;
    std::mutex cvMutex;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_SERVICES_ABSTRACTHEALTHCHECKSERVICE_HPP_

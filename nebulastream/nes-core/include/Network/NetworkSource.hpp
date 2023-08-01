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

#ifndef NES_CORE_INCLUDE_NETWORK_NETWORKSOURCE_HPP_
#define NES_CORE_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

#include <Network/NetworkForwardRefs.hpp>
#include <Network/NodeLocation.hpp>
#include <Runtime/Events.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/DataSource.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>

namespace NES::Network {

/**
 * @brief this class provide a zmq as data source
 */
class NetworkSource : public DataSource {

  public:
    NetworkSource(SchemaPtr schema,
                  Runtime::BufferManagerPtr bufferManager,
                  Runtime::QueryManagerPtr queryManager,
                  NetworkManagerPtr networkManager,
                  NesPartition nesPartition,
                  NodeLocation sinkLocation,
                  size_t numSourceLocalBuffers,
                  std::chrono::milliseconds waitTime,
                  uint8_t retryTimes,
                  std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief this method is just dummy and is replaced by the ZmqServer in the NetworkStack. Do not use!
     * @return TupleBufferPtr containing the received buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method
     * @return returns string describing the network source
     */
    std::string toString() const override;

    /**
      * @brief This method is called once an event is triggered for the current source.
      * The event type is PropagateEpochEvent. Method passes epoch barrier further to network sink as a reconfiguration message.
      * @param event
      */
    void onEvent(Runtime::BaseEvent& event) override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * It registers the source on the NetworkManager
     * @return true if registration on the network stack is successful
     */
    bool start() final;

    /**
     * @brief This method is overridden here to prevent the NetworkSource to start a thread.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool stop(Runtime::QueryTerminationType = Runtime::QueryTerminationType::Graceful) final;

    /**
     * @brief This method is overridden here to manage failures of NetworkSource.
     * It de-registers the source on the NetworkManager
     * @return true if deregistration on the network stack is successful
     */
    bool fail() final;

    /**
     * @brief This method is overridden here to prevent the NetworkSoure to start a thread.
     * @param bufferManager
     * @param queryManager
     */
    static void runningRoutine(const Runtime::BufferManagerPtr&, const Runtime::QueryManagerPtr&);

    /**
     * @brief This method is invoked when the source receives a reconfiguration message.
     * @param message the reconfiguration message
     * @param context thread context
     */
    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override;

    /**
     * @brief This method is invoked when the source receives a reconfiguration message.
     * @param message the reconfiguration message
     */
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override;

    /**
     * @brief API method called upon receiving an event, send event further upstream via Network Channel.
     * @param event
     * @param workerContext
     */
    void onEvent(Runtime::BaseEvent& event, Runtime::WorkerContextRef workerContext) override;

    /**
     * @brief
     * @param terminationType
     */
    void onEndOfStream(Runtime::QueryTerminationType terminationType) override;

    bool bind();

    friend bool operator<(const NetworkSource& lhs, const NetworkSource& rhs) { return lhs.nesPartition < rhs.nesPartition; }

  private:
    NetworkManagerPtr networkManager;
    NesPartition nesPartition;
    NodeLocation sinkLocation;
    // for event channel
    const std::chrono::milliseconds waitTime;
    const uint8_t retryTimes;
};

}// namespace NES::Network

#endif// NES_CORE_INCLUDE_NETWORK_NETWORKSOURCE_HPP_

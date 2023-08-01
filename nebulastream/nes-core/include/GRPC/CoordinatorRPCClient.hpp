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

#ifndef NES_CORE_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_
#define NES_CORE_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_

#include <Common/Identifiers.hpp>
#include <CoordinatorRPCService.grpc.pb.h>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/TimeMeasurement.hpp>
#include <grpcpp/grpcpp.h>
#include <optional>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

namespace NES {
class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Monitoring {
class RegistrationMetrics;
}// namespace Monitoring

namespace Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
class Waypoint;
using WaypointPtr = std::shared_ptr<Waypoint>;
enum class NodeType;
}// namespace Spatial::Index::Experimental

namespace Spatial::Mobility::Experimental {
struct ReconnectPrediction;
}

/**
 * @brief This class provides utility to interact with NES coordinator over RPC interface.
 */
class CoordinatorRPCClient {

  public:
    /**
     * @brief
     * @param address
     * @param retryAttempts: number of attempts for connecting
     * @param backOffTimeMs: backoff time to wait after a failed connection attempt
     */
    explicit CoordinatorRPCClient(const std::string& address,
                                  uint32_t rpcRetryAttemps = 10,
                                  std::chrono::milliseconds rpcBackoff = std::chrono::milliseconds(50));

    /**
     * @brief this methods registers physical sources provided by the node at the coordinator
     * @param physicalSources list of physical sources to register
     * @return bool indicating success
     */
    bool registerPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources);

    /**
     * @brief this method registers logical source via the coordinator
     * @param logicalSourceName of new logical source name
     * @param filePath to the file containing the schema
     * @return bool indicating the success of the log source
     * @note the logical source is not saved in the worker as it is maintained on the coordinator and all logical source can be
     * retrieved from the physical source map locally, if we later need the data we can add a map
     */
    bool registerLogicalSource(const std::string& logicalSourceName, const std::string& filePath);

    /**
     * @brief this method removes the logical source in the coordinator
     * @param logicalSourceName name of the logical source to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterLogicalSource(const std::string& logicalSourceName);

    /**
     * @brief this method removes a physical source from a logical source in the coordinator
     * @param logicalSourceName name of the logical source
     * @param physicalSourceName name of the physical source to be deleted
     * @return bool indicating success of the removal
     */
    bool unregisterPhysicalSource(const std::string& logicalSourceName, const std::string& physicalSourceName);

    /**
     * @brief method to add a new parent to an existing node
     * @param newParentId
     * @return bool indicating success
     */
    bool addParent(uint64_t parentId);

    /**
     * @brief method to replace old with new parent
     * @param oldParentId id of the old parent
     * @param newParentId id of the new parent
     * @return bool indicating success
     */
    bool replaceParent(uint64_t oldParentId, uint64_t newParentId);

    /**
     * @brief method to remove a parent from a node
     * @param parentId: id of the parent to be removed
     * @return bool indicating success
     */
    bool removeParent(uint64_t parentId);

    /**
     * @brief method to register a node after the connection is established
     * @param ipAddress: where this node is listening
     * @param grpcPort: the grpc port of the node
     * @param dataPort: the data port of the node
     * @param numberOfSlots: processing slots capacity
     * @param registrationMetrics: metrics to report
     * @param fixedCoordinates: the fixed geographical location of a non mobile node if it is known
     * @param isMobile: indicates if this worker is running on a fixed location device or on a mobile device
     * @return bool indicating success
     */
    bool registerNode(const std::string& ipAddress,
                      int64_t grpcPort,
                      int64_t dataPort,
                      int16_t numberOfSlots,
                      const Monitoring::RegistrationMetrics& registrationMetrics,
                      NES::Spatial::Index::Experimental::Location fixedCoordinates,
                      NES::Spatial::Index::Experimental::NodeType spatialType,
                      bool isTfInstalled);

    /**
     * @brief method to check if the coordinator is alive
     * @param healthServiceName name of the service
     * @return bool indicating success
     */
    bool checkCoordinatorHealth(std::string healthServiceName);

    /**
   * @brief method to unregister a node after the connection is established
   * @return bool indicating success
   */
    bool unregisterNode();

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    uint64_t getId() const;

    /**
     * @brief method to let the Coordinator know of the failure of a query
     * @param queryId: Query Id of failed Query
     * @param subQueryId: subQuery Id of failed Query
     * @param workerId: workerId where the Query failed
     * @param operatorId: operator Id of failed Query
     * @param errorMsg: more information about failure of the Query
     * @return bool indicating success
     */
    bool notifyQueryFailure(uint64_t queryId, uint64_t subQueryId, uint64_t workerId, uint64_t operatorId, std::string errorMsg);

    /**
      * @brief method to propagate new epoch timestamp to coordinator
      * @param timestamp: max timestamp of current epoch
      * @param queryId: identifies what query sends punctuation
      * @return bool indicating success
      */
    bool notifyEpochTermination(uint64_t timestamp, uint64_t queryId);

    /**
     * Experimental
     * @brief Method to get all field nodes (field nodes = non-mobile nodes with a specified geographical location) within a certain range around a geographical point
     * @param coord: center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding fixed coordinates as Location objects
     */
    std::vector<std::pair<uint64_t, NES::Spatial::Index::Experimental::Location>>
    getNodeIdsInRange(NES::Spatial::Index::Experimental::LocationPtr location, double radius);

    /**
     * @brief method to let the Coordinator know of errors and exceptions
     * @param workerId
     * @param errorMsg
     * @return bool indicating success
     */
    bool sendErrors(uint64_t workerId, std::string errorMsg);

    /**
     * Checks and mark the query for soft stop
     * @param queryId : the query id for which soft stop to be performed
     * @return true if coordinator marks the query for soft stop else false
     */
    bool checkAndMarkForSoftStop(QueryId queryId, QuerySubPlanId subPlanId, OperatorId sourceId);

    /**
     * Notify coordinator that for a subquery plan the soft stop is triggered or not
     * @param queryId: the query id to which the subquery plan belongs to
     * @param querySubPlanId: the query sub plan id
     * @param sourceId: the source id
     * @return true if coordinator successfully recorded the information else false
     */
    bool notifySourceStopTriggered(QueryId queryId,
                                   QuerySubPlanId querySubPlanId,
                                   OperatorId sourceId,
                                   Runtime::QueryTerminationType queryTermination);

    /**
     * Notify coordinator that for a subquery plan the soft stop is completed or not
     * @param queryId: the query id to which the subquery plan belongs to
     * @param querySubPlanId: the query sub plan id
     * @return true if coordinator successfully recorded the information else false
     */
    bool notifySoftStopCompleted(QueryId queryId, QuerySubPlanId querySubPlanId);

    /**
     * @brief this method is used by a mobile worker to inform the coordinator that location or time of the next expected reconnect
     * have changed or that the worker expects a reconnect to a different parent than the previously scheduled one
     * @param nodeId The id of the calling worker
     * @param scheduledReconnect a tuple containing the id of the new parent. The location where the reconnect is scheduled to happen
     * and the expected time of the reconnect
     * @return true if the information was succesfully saved at coordinator side
     */
    bool sendReconnectPrediction(uint64_t nodeId, NES::Spatial::Mobility::Experimental::ReconnectPrediction scheduledReconnect);

    /**
     * @brief this method can be called by a mobile worker to tell the coordinator, that the mobile devices position has changed
     * @param nodeId the id of the calling worker
     * @param locationUpdate a tuple containing the mobile devices location and a timestamp indicating when the device was located
     * at the transmitted position
     * @return true if the information has benn succesfully processed
     */
    bool sendLocationUpdate(uint64_t nodeId, NES::Spatial::Index::Experimental::WaypointPtr locationUpdate);

  private:
    uint64_t workerId;
    std::string address;
    std::shared_ptr<::grpc::Channel> rpcChannel;
    std::unique_ptr<CoordinatorRPCService::Stub> coordinatorStub;
    uint32_t rpcRetryAttemps;
    std::chrono::milliseconds rpcBackoff;
};
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

}// namespace NES
#endif// NES_CORE_INCLUDE_GRPC_COORDINATORRPCCLIENT_HPP_

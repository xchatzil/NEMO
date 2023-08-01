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

#ifndef NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_
#define NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_

#include <Nodes/Node.hpp>
#include <Spatial/Index/Location.hpp>
#include <Topology/LinkProperty.hpp>
#include <Util/TimeMeasurement.hpp>
#include <any>
#include <map>
#include <optional>

namespace NES {
class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

namespace Spatial::Index::Experimental {
using LocationPtr = std::shared_ptr<Location>;
class Waypoint;
using WaypointPtr = std::shared_ptr<Waypoint>;
enum class NodeType;
}// namespace Spatial::Index::Experimental

namespace Spatial::Mobility::Experimental {
class ReconnectSchedule;
using ReconnectSchedulePtr = std::shared_ptr<ReconnectSchedule>;
}// namespace Spatial::Mobility::Experimental

/**
 * @brief This class represents information about a physical node participating in the NES infrastructure
 */
class TopologyNode : public Node {

  public:
    static TopologyNodePtr
    create(uint64_t id, const std::string& ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources);
    virtual ~TopologyNode() = default;

    /**
     * @brief method to get the id of the node
     * @return id as a uint64_t
     */
    uint64_t getId() const;

    /**
     * @brief method to get the overall cpu capacity of the node
     * @return uint64_t cpu capacity
     */
    uint16_t getAvailableResources() const;

    /**
     * @brief method to reduce the cpu capacity of the node
     * @param uint64_t of the value that has to be subtracted
     */
    void reduceResources(uint16_t usedCapacity);

    /**
     * @brief method to increase CPU capacity
     * @param uint64_t of the vlaue that has to be added
     */
    void increaseResources(uint16_t freedCapacity);

    /**
     * @brief Get ip address of the node
     * @return ip address
     */
    std::string getIpAddress() const;

    /**
     * @brief Get grpc port for the node
     * @return grpc port
     */
    uint32_t getGrpcPort() const;

    /**
     * @brief Get the data port for the node
     * @return data port
     */
    uint32_t getDataPort() const;

    /**
     * @brief Get maintenance flag where 1 represents marked for maintenance
     * @return bool
     */
    bool getMaintenanceFlag() const;

    /**
     * @brief sets maintenance flag where 1 represents marked for maintenance
     * @param flag
     */
    void setMaintenanceFlag(bool flag);

    std::string toString() const override;

    /**
     * @brief Create a shallow copy of the physical node i.e. without copying the parent and child nodes
     * @return
     */
    TopologyNodePtr copy();

    explicit TopologyNode(uint64_t id, std::string ipAddress, uint32_t grpcPort, uint32_t dataPort, uint16_t resources);

    bool containAsParent(NodePtr node) override;

    bool containAsChild(NodePtr node) override;

    /**
     * @brief Add a new property to the stored properties map
     * @param key key of the new property
     * @param value value of the new property
     */
    void addNodeProperty(const std::string& key, const std::any& value);

    /**
     * @brief Check if a Node property exists
     * @param key key of the property
     * @return true if the property with the given key exists
     */
    bool hasNodeProperty(const std::string& key);

    /**
     * @brief Get a the value of a property
     * @param key key of the value to retrieve
     * @return value of the property with the given key
     */
    std::any getNodeProperty(const std::string& key);

    /**
     * @brief Remove a property from the stored properties map
     * @param key key of the property to remove
     * @return true if the removal is successful
     */
    bool removeNodeProperty(const std::string& key);

    /**
     * @brief add a new link property to the stored properties map
     * @param linked topology node to which the property will be associated
     * @param value of the link property
     */
    void addLinkProperty(const TopologyNodePtr& linkedNode, const LinkPropertyPtr& topologyLink);

    /**
     * @brief get a the value of a link property
     * @param linked topology node associated with the link property to retrieve
     * @return value of the link property
     */
    LinkPropertyPtr getLinkProperty(const TopologyNodePtr& linkedNode);

    /**
     * @brief remove a a link property from the stored map
     * @param linked topology node associated with the link property to remove
     * @return true if the removal is successful
     */
    bool removeLinkProperty(const TopologyNodePtr& linkedNode);

    /**
     * Experimental
     * @brief get the geographical coordinates of this topology node.
     * @return The geographical coordinates of the node in case the node is a field node. nullopt_t otherwise
     */
    NES::Spatial::Index::Experimental::WaypointPtr getCoordinates();

    /**
     * Experimental
     * @return a smart pointer to this nodes reconnect schedule or nullptr if the node is not a mobile node
     */
    NES::Spatial::Mobility::Experimental::ReconnectSchedulePtr getReconnectSchedule();

    /**
     * Experimental
     * @brief set the fixed geographical coordinates of this topology node, making it a field node
     * @param lat: geographical latitude in signed degrees [-90, 90]
     * @param lng: geographical longitude in signed degrees [-180, 180]
     * @return true on success
     */
    void setFixedCoordinates(double latitude, double longitude);

    /**
     * Experimental
     * @brief set the fixed geographical coordinates of this topology node, making it a field node
     * @param geoLoc: the Geographical location of the node
     * @return true on success
     */
    void setFixedCoordinates(NES::Spatial::Index::Experimental::Location geoLoc);

    /**
     * Experimental
     * @brief sets the status of the node as running on a mobile device or a fixed location device.
     * To be run right after node creation. Fixed nodes should not become mobile or vice versa at a later point
     * @param workerSpatialType
     */
    void setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType workerSpatialType);

    /**
     * Experimental
     * @brief check if the node is a running on a mobile device or not
     * @return true if the node is running on a mobile device
     */
    NES::Spatial::Index::Experimental::NodeType getSpatialNodeType();

  private:
    uint64_t id;
    std::string ipAddress;
    uint32_t grpcPort;
    uint32_t dataPort;
    uint16_t resources;
    uint16_t usedResources;
    bool maintenanceFlag;
    NES::Spatial::Index::Experimental::Location fixedCoordinates;
    NES::Spatial::Index::Experimental::NodeType spatialType;

    /**
     * @brief A field to store a map of node properties
     */
    std::map<std::string, std::any> nodeProperties;

    /**
     * @brief A field to store a map of the id of the linked nodes and its link property
     */
    std::map<uint64_t, LinkPropertyPtr> linkProperties;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_TOPOLOGY_TOPOLOGYNODE_HPP_

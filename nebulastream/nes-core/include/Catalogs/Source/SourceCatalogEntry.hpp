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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGENTRY_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGENTRY_HPP_

#include <memory>
#include <sstream>
#include <string>

namespace NES {

class TopologyNode;
using TopologyNodePtr = std::shared_ptr<TopologyNode>;

class LogicalSource;
using LogicalSourcePtr = std::shared_ptr<LogicalSource>;

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Catalogs::Source {

class SourceCatalogEntry;
using SourceCatalogEntryPtr = std::shared_ptr<SourceCatalogEntry>;

/**
 * @brief one entry in the catalog contains
 *    - the physical data source that can be consumed
 *    - The logical source to which the physical source contributes towards
 *    - the topology that offer this source
 * @Limitations
 *
 */
class SourceCatalogEntry {

  public:
    /**
     * @brief Create the shared pointer for the source catalog entry
     * @param physicalSource: physical source name
     * @param logicalSource: the logical source name
     * @param topologyNode: the topology topologyNode
     * @return shared pointer to Source catalog entry
     */
    static SourceCatalogEntryPtr
    create(PhysicalSourcePtr physicalSource, LogicalSourcePtr logicalSource, TopologyNodePtr topologyNode);

    explicit SourceCatalogEntry(PhysicalSourcePtr physicalSource, LogicalSourcePtr logicalSource, TopologyNodePtr topologyNode);

    /**
     * @brief Get the physical source
     * @return the shared pointer to the physical source
     */
    const PhysicalSourcePtr& getPhysicalSource() const;

    /**
     * @brief Get the logical source
     * @return the shared pointer to the logical source
     */
    const LogicalSourcePtr& getLogicalSource() const;

    /**
     * @brief Get the topology node
     * @return the shared pointer to the topology node
     */
    const TopologyNodePtr& getNode() const;

    /**
     * @brief Get the string rep of the source catalog entry
     * @return string rep of the source catalog entry
     */
    std::string toString();

  private:
    PhysicalSourcePtr physicalSource;
    LogicalSourcePtr logicalSource;
    TopologyNodePtr node;
};
}// namespace Catalogs::Source
}// namespace NES

#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_SOURCECATALOGENTRY_HPP_

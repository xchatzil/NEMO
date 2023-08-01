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

#ifndef NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYNODE_HPP_
#define NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYNODE_HPP_

#include <Common/Identifiers.hpp>
#include <Operators/OperatorNode.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class GlobalQueryNode;
using GlobalQueryNodePtr = std::shared_ptr<GlobalQueryNode>;

/**
 * @brief This class encapsulates the logical operator shared by a set of queryIdAndCatalogEntryMapping.
 */
class GlobalQueryNode : public Node {

  public:
    virtual ~GlobalQueryNode() = default;

    /**
     * @brief Creates empty global query node
     * @param id: id of the global query node
     */
    static GlobalQueryNodePtr createEmpty(uint64_t id);

    /**
     * @brief Global Query Operator builder
     * @param id: id of the global query node
     * @param queryId: query id of the query
     * @param operatorNode: logical operator
     * @return Shared pointer to the instance of Global Query Operator instance
     */
    static GlobalQueryNodePtr create(uint64_t id, OperatorNodePtr operatorNode);

    /**
     * @brief Get id of the node
     * @return node id
     */
    uint64_t getId() const;

    /**
     * @brief Check if logical operator already present in the node
     * @param operatorNode : logical operator to check
     * @return returns an operator which is same as input operator else nullptr.
     */
    OperatorNodePtr hasOperator(OperatorNodePtr operatorNode);

    /**
     * @brief helper function of get global query nodes with specific logical operator type
     */
    template<class T>
    void getNodesWithTypeHelper(std::vector<GlobalQueryNodePtr>& foundNodes) {
        NES_INFO("GlobalQueryNode: Get the global query node containing operator of specific type");
        if (operatorNode->instanceOf<T>()) {
            foundNodes.push_back(shared_from_this()->as<GlobalQueryNode>());
        }

        NES_INFO("GlobalQueryNode: Find if the child global query nodes of this node containing operator of specific type");
        for (auto& successor : this->children) {
            successor->as<GlobalQueryNode>()->getNodesWithTypeHelper<T>(foundNodes);
        }
    }

    /**
     * @brief Get registered operator
     * @return operator node
     */
    OperatorNodePtr getOperator();

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] std::string toString() const override;

  private:
    explicit GlobalQueryNode(uint64_t id);
    GlobalQueryNode(uint64_t id, OperatorNodePtr operatorNode);

    uint64_t id;
    OperatorNodePtr operatorNode;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYNODE_HPP_

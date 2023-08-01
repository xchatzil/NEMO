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
#include <Operators/OperatorNode.hpp>
#include <Util/UtilityFunctions.hpp>
#include <algorithm>
#include <utility>

namespace NES {
/**
 * @brief We initialize the input and output schemas with empty schemas.
 */
OperatorNode::OperatorNode(OperatorId id) : id(id), properties() { NES_INFO("Creating Operator " << id); }

OperatorId OperatorNode::getId() const { return id; }

void OperatorNode::setId(OperatorId id) { OperatorNode::id = id; }

bool OperatorNode::hasMultipleChildrenOrParents() {
    //has multiple child operator
    bool hasMultipleChildren = (!getChildren().empty()) && getChildren().size() > 1;
    //has multiple parent operator
    bool hasMultipleParent = (!getParents().empty()) && getParents().size() > 1;
    NES_DEBUG("OperatorNode: has multiple children " << hasMultipleChildren << " or has multiple parent " << hasMultipleParent);
    return hasMultipleChildren || hasMultipleParent;
}

bool OperatorNode::hasMultipleChildren() { return !getChildren().empty() && getChildren().size() > 1; }

bool OperatorNode::hasMultipleParents() { return !getParents().empty() && getParents().size() > 1; }

OperatorNodePtr OperatorNode::duplicate() {
    NES_INFO("OperatorNode: Create copy of the operator");
    const OperatorNodePtr copyOperator = copy();

    NES_DEBUG("OperatorNode: copy all parents");
    for (const auto& parent : getParents()) {
        if (!copyOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add parent to copy");
        }
    }

    NES_DEBUG("OperatorNode: copy all children");
    for (const auto& child : getChildren()) {
        if (!copyOperator->addChild(getDuplicateOfChild(child->as<OperatorNode>()->duplicate()))) {
            NES_THROW_RUNTIME_ERROR("OperatorNode: Unable to add child to copy");
        }
    }
    return copyOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfParent(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    const OperatorNodePtr& copyOfOperator = operatorNode->copy();
    if (operatorNode->getParents().empty()) {
        NES_TRACE("OperatorNode: No ancestor of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all parents get copy of the ancestor and add as parent to the copy of the input operator");
    for (const auto& parent : operatorNode->getParents()) {
        copyOfOperator->addParent(getDuplicateOfParent(parent->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

OperatorNodePtr OperatorNode::getDuplicateOfChild(const OperatorNodePtr& operatorNode) {
    NES_DEBUG("OperatorNode: create copy of the input operator");
    OperatorNodePtr copyOfOperator = operatorNode->copy();
    if (operatorNode->getChildren().empty()) {
        NES_TRACE("OperatorNode: No children of the input node. Returning the copy of the input operator");
        return copyOfOperator;
    }

    NES_TRACE("OperatorNode: For all children get copy of their children and add as child to the copy of the input operator");
    for (const auto& child : operatorNode->getChildren()) {
        copyOfOperator->addChild(getDuplicateOfParent(child->as<OperatorNode>()));
    }
    NES_TRACE("OperatorNode: return copy of the input operator");
    return copyOfOperator;
}

bool OperatorNode::addChild(NodePtr newNode) {

    if (!newNode) {
        NES_ERROR("OperatorNode: Can't add null node");
        return false;
    }

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as child to itself");
        return false;
    }

    std::vector<NodePtr> currentChildren = getChildren();
    auto found = std::find_if(currentChildren.begin(), currentChildren.end(), [&](const NodePtr& child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentChildren.end()) {
        NES_DEBUG("OperatorNode: Adding node to the children.");
        children.push_back(newNode);
        newNode->addParent(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its children so skip add child operation.");
    return false;
}

bool OperatorNode::addParent(NodePtr newNode) {

    if (!newNode) {
        NES_ERROR("OperatorNode: Can't add null node");
        return false;
    }

    if (newNode->as<OperatorNode>()->getId() == id) {
        NES_ERROR("OperatorNode: can not add self as parent to itself");
        return false;
    }

    std::vector<NodePtr> currentParents = getParents();
    auto found = std::find_if(currentParents.begin(), currentParents.end(), [&](const NodePtr& child) {
        return child->as<OperatorNode>()->getId() == newNode->as<OperatorNode>()->getId();
    });

    if (found == currentParents.end()) {
        NES_DEBUG("OperatorNode: Adding node to the Parents.");
        parents.push_back(newNode);
        newNode->addChild(shared_from_this());
        return true;
    }
    NES_DEBUG("OperatorNode: the node is already part of its parent so skip add parent operation.");
    return false;
}

NodePtr OperatorNode::getChildWithOperatorId(uint64_t operatorId) {

    if (id == operatorId) {
        return shared_from_this();
    }
    for (auto& child : children) {
        auto found = child->as<OperatorNode>()->getChildWithOperatorId(operatorId);
        if (found) {
            return found;
        }
    }
    return nullptr;
}

void OperatorNode::addProperty(const std::string& key, const std::any value) { properties[key] = value; }

std::any OperatorNode::getProperty(const std::string& key) { return properties[key]; }

bool OperatorNode::hasProperty(const std::string& key) { return properties.find(key) != properties.end(); }

void OperatorNode::removeProperty(const std::string& key) { properties.erase(key); }

}// namespace NES

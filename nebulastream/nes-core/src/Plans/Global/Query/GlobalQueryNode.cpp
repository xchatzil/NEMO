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

#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <algorithm>
#include <utility>

namespace NES {

GlobalQueryNode::GlobalQueryNode(uint64_t id) : id(id) {}

GlobalQueryNode::GlobalQueryNode(uint64_t id, OperatorNodePtr operatorNode) : id(id), operatorNode(std::move(operatorNode)) {}

GlobalQueryNodePtr GlobalQueryNode::createEmpty(uint64_t id) { return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id)); }

GlobalQueryNodePtr GlobalQueryNode::create(uint64_t id, OperatorNodePtr operatorNode) {
    return std::make_shared<GlobalQueryNode>(GlobalQueryNode(id, std::move(operatorNode)));
}

uint64_t GlobalQueryNode::getId() const { return id; }

OperatorNodePtr GlobalQueryNode::hasOperator(OperatorNodePtr operatorNode) {
    NES_DEBUG("GlobalQueryNode: Check if a similar logical operator present in the global query node " << id);
    if (this->operatorNode && this->operatorNode->equal(operatorNode)) {
        return operatorNode;
    }
    return nullptr;
}

std::string GlobalQueryNode::toString() const {
    return "Operator [" + operatorNode->toString() + "], Global Query Node Id [" + std::to_string(id) + "]";
}

OperatorNodePtr GlobalQueryNode::getOperator() { return operatorNode; }

bool GlobalQueryNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<GlobalQueryNode>()) {
        return id == rhs->as<GlobalQueryNode>()->getId();
    }
    return false;
}

}// namespace NES
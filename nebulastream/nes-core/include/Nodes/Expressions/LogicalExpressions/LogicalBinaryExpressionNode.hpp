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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/BinaryExpressionNode.hpp>
#include <Nodes/Expressions/LogicalExpressions/LogicalExpressionNode.hpp>
namespace NES {
/**
 * @brief This node represents a logical binary expression.
 */
class LogicalBinaryExpressionNode : public BinaryExpressionNode, public LogicalExpressionNode {
  public:
    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override = 0;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

  protected:
    LogicalBinaryExpressionNode();
    ~LogicalBinaryExpressionNode() = default;
    explicit LogicalBinaryExpressionNode(LogicalBinaryExpressionNode* other);
};
}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_LOGICALEXPRESSIONS_LOGICALBINARYEXPRESSIONNODE_HPP_

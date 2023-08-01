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

#ifndef NES_CORE_INCLUDE_NODES_EXPRESSIONS_FIELDRENAMEEXPRESSIONNODE_HPP_
#define NES_CORE_INCLUDE_NODES_EXPRESSIONS_FIELDRENAMEEXPRESSIONNODE_HPP_
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
namespace NES {

/**
 * @brief A FieldRenameExpressionNode allows us to rename an attribute value via .as in the query
 */
class FieldRenameExpressionNode : public ExpressionNode {
  public:
    /**
     * @brief Create FieldRename Expression node
     * @param fieldName : name of the field
     * @param newFieldName : new name of the field
     * @param datatype : the data type
     * @return pointer to the FieldRenameExpressionNode
     */
    static ExpressionNodePtr create(FieldAccessExpressionNodePtr originalField, std::string newFieldName);

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    std::string getNewFieldName();

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) override;

    /**
    * @brief Create a deep copy of this expression node.
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr copy() override;

    FieldAccessExpressionNodePtr getOriginalField() const;

  protected:
    explicit FieldRenameExpressionNode(FieldRenameExpressionNode* other);

  private:
    FieldRenameExpressionNode(const FieldAccessExpressionNodePtr& originalField, std::string newFieldName);

    FieldAccessExpressionNodePtr originalField;
    std::string newFieldName;
};

using FieldRenameExpressionNodePtr = std::shared_ptr<FieldRenameExpressionNode>;

}// namespace NES

#endif// NES_CORE_INCLUDE_NODES_EXPRESSIONS_FIELDRENAMEEXPRESSIONNODE_HPP_

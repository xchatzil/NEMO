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

#ifndef NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATORNODE_HPP_

#include <Operators/OperatorForwardDeclaration.hpp>
#include <Operators/OperatorNode.hpp>

namespace NES {

/**
 * @brief A unary operator with one input operator, it has exactly one input operator.
 * This virtually inheritances for OperatorNode
 * https://en.wikipedia.org/wiki/Virtual_inheritance
 */
class UnaryOperatorNode : public virtual OperatorNode {
  public:
    explicit UnaryOperatorNode(OperatorId id);
    ~UnaryOperatorNode() noexcept override = default;

    /**
      * @brief detect if this operator is a binary operator, i.e., it has two children
      * @return true if n-ary else false;
      */
    bool isBinaryOperator() const override;

    /**
    * @brief detect if this operator is an uary operator, i.e., it has only one child
    * @return true if n-ary else false;
    */
    bool isUnaryOperator() const override;

    /**
   * @brief detect if this operator is an exchange operator, i.e., it sends it output to multiple parents
   * @return true if n-ary else false;
   */
    bool isExchangeOperator() const override;

    /**
   * @brief get the input schema of this operator
   * @return SchemaPtr
   */
    SchemaPtr getInputSchema() const;

    /**
     * @brief set the input schema of this operator
     * @param inputSchema
    */
    void setInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() const override;

    /**
     * @brief set the result schema of this operator
     * @param outputSchema
    */
    void setOutputSchema(SchemaPtr outputSchema) override;

    /**
     * @brief Set the input origin ids from the input stream
     * @param originIds
     */
    void setInputOriginIds(std::vector<OriginId> originIds);

    /**
     * @brief Gets the input origin ids  from the input stream
     * @return std::vector<OriginId>
     */
    std::vector<OriginId> getInputOriginIds();

    /**
     * @brief Gets the output origin ids from this operator
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getOutputOriginIds() override;

  protected:
    SchemaPtr inputSchema;
    SchemaPtr outputSchema;
    std::vector<OriginId> inputOriginIds;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_UNARYOPERATORNODE_HPP_

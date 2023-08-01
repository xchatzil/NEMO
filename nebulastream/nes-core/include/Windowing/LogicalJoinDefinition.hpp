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

#ifndef NES_CORE_INCLUDE_WINDOWING_LOGICALJOINDEFINITION_HPP_
#define NES_CORE_INCLUDE_WINDOWING_LOGICALJOINDEFINITION_HPP_
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <cstdint>

namespace NES::Join {

/**
 * @brief Runtime definition of a join operator
 * @experimental
 */
class LogicalJoinDefinition {

  public:
    /**
     * With this enum we distinguish between options to compose two sources, in particular, we reuse Join Logic for binary CEP operators which require a Cartesian product.
     * Thus, INNER_JOIN combines two tuples in case they share a common key attribute
     * CARTESIAN_PRODUCT combines two tuples regardless if they share a common attribute.
     *
     * Example:
     * Source1: {(key1,2),(key2,3)}
     * Source2: {(key1,2),(key2,3)}
     *
     * INNER_JOIN: {(Key1,2,2), (key2,3,3)}
     * CARTESIAN_PRODUCT: {(key1,2,key1,2),(key1,2,key2,3), (key2,3,key1,2), (key2,3,key2,3)}
     *
     */
    enum JoinType { INNER_JOIN, CARTESIAN_PRODUCT };
    static LogicalJoinDefinitionPtr create(const FieldAccessExpressionNodePtr& leftJoinKeyType,
                                           const FieldAccessExpressionNodePtr& rightJoinKeyType,
                                           const Windowing::WindowTypePtr& windowType,
                                           const Windowing::DistributionCharacteristicPtr& distributionType,
                                           const Windowing::WindowTriggerPolicyPtr& triggerPolicy,
                                           const BaseJoinActionDescriptorPtr& triggerAction,
                                           uint64_t numberOfInputEdgesLeft,
                                           uint64_t numberOfInputEdgesRight,
                                           JoinType joinType);

    explicit LogicalJoinDefinition(FieldAccessExpressionNodePtr leftJoinKeyType,
                                   FieldAccessExpressionNodePtr rightJoinKeyType,
                                   Windowing::WindowTypePtr windowType,
                                   Windowing::DistributionCharacteristicPtr distributionType,
                                   Windowing::WindowTriggerPolicyPtr triggerPolicy,
                                   BaseJoinActionDescriptorPtr triggerAction,
                                   uint64_t numberOfInputEdgesLeft,
                                   uint64_t numberOfInputEdgesRight,
                                   JoinType joinType);

    /**
    * @brief getter/setter for on left join key
    */
    FieldAccessExpressionNodePtr getLeftJoinKey();

    /**
   * @brief getter/setter for on left join key
   */
    FieldAccessExpressionNodePtr getRightJoinKey();

    /**
   * @brief getter left source type
   */
    SchemaPtr getLeftSourceType();

    /**
   * @brief getter of right source type
   */
    SchemaPtr getRightSourceType();

    /**
     * @brief getter/setter for window type
    */
    Windowing::WindowTypePtr getWindowType();

    /**
     * @brief getter for on trigger policy
     */
    [[nodiscard]] Windowing::WindowTriggerPolicyPtr getTriggerPolicy() const;

    /**
     * @brief getter for on trigger action
     * @return trigger action
    */
    [[nodiscard]] BaseJoinActionDescriptorPtr getTriggerAction() const;

    /**
     * @brief getter for on trigger action
     * @return trigger action
    */
    [[nodiscard]] JoinType getJoinType() const;

    /**
     * @brief getter for on distribution type
     * @return distributionType
    */
    [[nodiscard]] Windowing::DistributionCharacteristicPtr getDistributionType() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @experimental This is experimental API
     * @return
     */
    uint64_t getNumberOfInputEdgesLeft() const;

    /**
     * @brief number of input edges. Need to define a clear concept for this
     * @return
     */
    uint64_t getNumberOfInputEdgesRight() const;

    /**
     * @brief Update the left and right source types upon type inference
     * @param leftSourceType the type of the left source
     * @param rightSourceType the type of the right source
     */
    void updateSourceTypes(SchemaPtr leftSourceType, SchemaPtr rightSourceType);

    /**
     * @brief Update the output source type upon type inference
     * @param outputSchema the type of the output source
     */
    void updateOutputDefinition(SchemaPtr outputSchema);

    /**
     * @brief Getter of the output source schema
     * @return the output source schema
     */
    [[nodiscard]] SchemaPtr getOutputSchema() const;

    void setNumberOfInputEdgesLeft(uint64_t numberOfInputEdgesLeft);
    void setNumberOfInputEdgesRight(uint64_t numberOfInputEdgesRight);

  private:
    FieldAccessExpressionNodePtr leftJoinKeyType;
    FieldAccessExpressionNodePtr rightJoinKeyType;
    SchemaPtr leftSourceType{nullptr};
    SchemaPtr rightSourceType{nullptr};
    SchemaPtr outputSchema{nullptr};
    Windowing::WindowTriggerPolicyPtr triggerPolicy;
    BaseJoinActionDescriptorPtr triggerAction;
    Windowing::WindowTypePtr windowType;
    Windowing::DistributionCharacteristicPtr distributionType;
    uint64_t numberOfInputEdgesLeft;
    uint64_t numberOfInputEdgesRight;
    JoinType joinType;
};

using LogicalJoinDefinitionPtr = std::shared_ptr<LogicalJoinDefinition>;
}// namespace NES::Join
#endif// NES_CORE_INCLUDE_WINDOWING_LOGICALJOINDEFINITION_HPP_

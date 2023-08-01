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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_
#include <QueryCompiler/Phases/Translations/PhysicalOperatorProvider.hpp>
#include <vector>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultPhysicalOperatorProvider : public PhysicalOperatorProvider {
  public:
    DefaultPhysicalOperatorProvider(QueryCompilerOptionsPtr options);
    static PhysicalOperatorProviderPtr create(QueryCompilerOptionsPtr options);
    void lower(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode) override;
    virtual ~DefaultPhysicalOperatorProvider() noexcept = default;

  protected:
    /**
     * @brief Insets demultiplex operator before the current operator.
     * @param operatorNode
     */
    void insertDemultiplexOperatorsBefore(const LogicalOperatorNodePtr& operatorNode);
    /**
     * @brief Insert multiplex operator after the current operator.
     * @param operatorNode
     */
    void insertMultiplexOperatorsAfter(const LogicalOperatorNodePtr& operatorNode);
    /**
     * @brief Checks if the current operator is a demultiplexer, if it has multiple parents.
     * @param operatorNode
     * @return
     */
    bool isDemultiplex(const LogicalOperatorNodePtr& operatorNode);

    /**
     * @brief Lowers a binary operator
     * @param queryPlan current plan
     * @param operatorNode current operator
     */
    void lowerBinaryOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a unary operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnaryOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a union operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerUnionOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a project operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerProjectOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

#ifdef TFDEF
    /**
    * @brief Lowers an infer model operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerInferModelOperator(QueryPlanPtr queryPlan, LogicalOperatorNodePtr operatorNode);
#endif// TFDEF

    /**
    * @brief Lowers a map operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerMapOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a window operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWindowOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a thread local window operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerThreadLocalWindowOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a watermark assignment operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerWatermarkAssignmentOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a join operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerJoinOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a batch join operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerBatchJoinOperator(const QueryPlanPtr& queryPlan, const LogicalOperatorNodePtr& operatorNode);

    /**
    * @brief Lowers a join build operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    OperatorNodePtr getJoinBuildInputOperator(const JoinLogicalOperatorNodePtr& joinOperator,
                                              SchemaPtr schema,
                                              std::vector<OperatorNodePtr> children);

    // todo might need super class of join and batch join, this function is a duplicate of above
    static OperatorNodePtr getBatchJoinChildInputOperator(const Experimental::BatchJoinLogicalOperatorNodePtr& batchJoinOperator,
                                                          SchemaPtr outputSchema,
                                                          std::vector<OperatorNodePtr> children);

    /**
    * @brief Lowers a cep iteration operator
    * @param queryPlan current plan
    * @param operatorNode current operator
    */
    void lowerCEPIterationOperator(const QueryPlanPtr queryPlan, const LogicalOperatorNodePtr operatorNode);
};

}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTPHYSICALOPERATORPROVIDER_HPP_

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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_
#include <QueryCompiler/Phases/Translations/GeneratableOperatorProvider.hpp>
namespace NES {
namespace QueryCompilation {

/**
 * @brief Provides a set of default lowerings for logical operators to corresponding physical operators.
 */
class DefaultGeneratableOperatorProvider : public GeneratableOperatorProvider {
  public:
    static GeneratableOperatorProviderPtr create();
    void lower(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode) override;
    virtual ~DefaultGeneratableOperatorProvider() = default;

  protected:
    /**
     * @brief Lowers a source operator. In this case we perform no action, as physical source operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerSource(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a sink operator. In this case we perform no action, as physical sink operators can't be lowered.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a scan operator and creates a corresponding generatable buffer scan.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerScan(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a emit operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerEmit(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a projection operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerProjection(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a filter operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerFilter(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers an inferModel operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerInferModel(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a map operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerMap(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a watermark assignment operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerWatermarkAssignment(const QueryPlanPtr& queryPlan,
                                         const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a window sink operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerWindowSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
     * @brief Lowers a slice sink operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerSliceSink(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);

    /**
    * @brief Lowers a slice sink operator.
    * @param queryPlan the query plan
    * @param operatorNode the current operator node.
    */
    void lowerSliceMerging(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a slice pre aggregation.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerSlicePreAggregation(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a thread local slice pre aggregation for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerKeyedThreadLocalSlicePreAggregation(const QueryPlanPtr& queryPlan,
                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the thread local slice merge operator for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerKeyedSliceMergingOperator(const QueryPlanPtr& queryPlan,
                                        const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the window sink for keyed tumbling windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerKeyedTumblingWindowSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the window sink for keyed sliding windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerKeyedSlidingWindowSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the global slice store append operator for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerKeyedGlobalSliceStoreAppendOperator(const QueryPlanPtr& queryPlan,
                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a thread local slice pre aggregation for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerGlobalThreadLocalSlicePreAggregation(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the thread local slice merge operator for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerGlobalSliceMergingOperator(const QueryPlanPtr& queryPlan,
                                         const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the window sink for keyed tumbling windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerGlobalTumblingWindowSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the window sink for keyed sliding windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerGlobalSlidingWindowSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers the global slice store append operator for keyed windows.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    void lowerGlobalWindowSliceStoreAppendOperator(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a join build operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerJoinBuild(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a join sink operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerJoinSink(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a batch join build operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerBatchJoinBuild(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a batch join sink operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static void lowerBatchJoinProbe(const QueryPlanPtr& queryPlan, const PhysicalOperators::PhysicalOperatorPtr& operatorNode);

    /**
     * @brief Lowers a window operator.
     * @param queryPlan the query plan
     * @param operatorNode the current operator node.
     */
    static GeneratableOperators::GeneratableWindowAggregationPtr
    lowerWindowAggregation(const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor);
    /**
   * @brief Lowers a cep iteration operator.
   * @param queryPlan the query plan
   * @param operatorNode the current operator node.
   */
    void lowerCEPIteration(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode);
};

}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DEFAULTGENERATABLEOPERATORPROVIDER_HPP_

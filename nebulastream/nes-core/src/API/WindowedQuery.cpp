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
#include <API/AttributeField.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/WatermarkStrategy.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>

#include <API/WindowedQuery.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <cstdarg>
#include <iostream>
#include <utility>

namespace NES {

WindowOperatorBuilder::WindowedQuery Query::window(const Windowing::WindowTypePtr& windowType) {
    return WindowOperatorBuilder::WindowedQuery(*this, windowType);
}

namespace WindowOperatorBuilder {
WindowedQuery::WindowedQuery(Query& originalQuery, Windowing::WindowTypePtr windowType)
    : originalQuery(originalQuery), windowType(std::move(windowType)) {}

KeyedWindowedQuery::KeyedWindowedQuery(Query& originalQuery,
                                       Windowing::WindowTypePtr windowType,
                                       std::vector<ExpressionNodePtr> keys)
    : originalQuery(originalQuery), windowType(std::move(windowType)), keys(keys) {}

}//namespace WindowOperatorBuilder

Query& Query::window(const Windowing::WindowTypePtr& windowType, std::vector<Windowing::WindowAggregationPtr> aggregation) {
    NES_DEBUG("Query: add window operator");
    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window

    uint64_t allowedLateness = 0;
    if (windowType->isTumblingWindow() || windowType->isSlidingWindow()) {
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowType);
        if (!queryPlan->getRootOperators()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
            NES_DEBUG("add default watermark strategy as non is provided");
            if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
                queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
            } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
                queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        Attribute(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        timeBasedWindowType->getTimeCharacteristic()->getTimeUnit())));
            }
        } else {
            NES_DEBUG("add existing watermark strategy for window");
            auto assigner = queryPlan->getRootOperators()[0]->as<WatermarkAssignerLogicalOperatorNode>();
            if (auto eventTimeWatermarkStrategyDescriptor =
                    std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(
                        assigner->getWatermarkStrategyDescriptor())) {
                allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
            } else if (auto ingestionTimeWatermarkDescriptior =
                           std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                               assigner->getWatermarkStrategyDescriptor())) {
                NES_WARNING("Note: ingestion time does not support allowed lateness yet");
            } else {
                NES_ERROR("cannot create watermark strategy from descriptor");
            }
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();

    auto windowDefinition =
        Windowing::LogicalWindowDefinition::create(aggregation,
                                                   windowType,
                                                   Windowing::DistributionCharacteristic::createCompleteWindowType(),
                                                   triggerPolicy,
                                                   triggerAction,
                                                   allowedLateness);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

Query& Query::windowByKey(std::vector<ExpressionNodePtr> onKeys,
                          const Windowing::WindowTypePtr& windowType,
                          std::vector<Windowing::WindowAggregationPtr> aggregation) {
    NES_DEBUG("Query: add keyed window operator");
    std::vector<FieldAccessExpressionNodePtr> expressionNodes;
    for (auto onKey : onKeys) {
        if (!onKey->instanceOf<FieldAccessExpressionNode>()) {
            NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + onKey->toString());
        }
        expressionNodes.emplace_back(onKey->as<FieldAccessExpressionNode>());
    }

    //we use a on time trigger as default that triggers on each change of the watermark
    auto triggerPolicy = Windowing::OnWatermarkChangeTriggerPolicyDescription::create();

    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    //numberOfInputEdges = 1, this will in a later rule be replaced with the number of children of the window

    uint64_t allowedLateness = 0;
    if (windowType->isTumblingWindow() || windowType->isSlidingWindow()) {
        auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowType);
        // check if query contain watermark assigner, and add if missing (as default behaviour)
        if (!queryPlan->getRootOperators()[0]->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
            NES_DEBUG("add default watermark strategy as non is provided");
            if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::IngestionTime) {
                queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                    Windowing::IngestionTimeWatermarkStrategyDescriptor::create()));
            } else if (timeBasedWindowType->getTimeCharacteristic()->getType() == Windowing::TimeCharacteristic::EventTime) {
                queryPlan->appendOperatorAsNewRoot(LogicalOperatorFactory::createWatermarkAssignerOperator(
                    Windowing::EventTimeWatermarkStrategyDescriptor::create(
                        Attribute(timeBasedWindowType->getTimeCharacteristic()->getField()->getName()),
                        API::Milliseconds(0),
                        timeBasedWindowType->getTimeCharacteristic()->getTimeUnit())));
            }
        } else {
            NES_DEBUG("add existing watermark strategy for window");
            auto assigner = queryPlan->getRootOperators()[0]->as<WatermarkAssignerLogicalOperatorNode>();
            if (auto eventTimeWatermarkStrategyDescriptor =
                    std::dynamic_pointer_cast<Windowing::EventTimeWatermarkStrategyDescriptor>(
                        assigner->getWatermarkStrategyDescriptor())) {
                allowedLateness = eventTimeWatermarkStrategyDescriptor->getAllowedLateness().getTime();
            } else if (auto ingestionTimeWatermarkDescriptior =
                           std::dynamic_pointer_cast<Windowing::IngestionTimeWatermarkStrategyDescriptor>(
                               assigner->getWatermarkStrategyDescriptor())) {
                NES_WARNING("Note: ingestion time does not support allowed lateness yet");
            } else {
                NES_ERROR("cannot create watermark strategy from descriptor");
            }
        }
    }

    auto inputSchema = getQueryPlan()->getRootOperators()[0]->getOutputSchema();

    auto windowDefinition =
        Windowing::LogicalWindowDefinition::create(expressionNodes,
                                                   aggregation,
                                                   windowType,
                                                   Windowing::DistributionCharacteristic::createCompleteWindowType(),
                                                   triggerPolicy,
                                                   triggerAction,
                                                   allowedLateness);
    auto windowOperator = LogicalOperatorFactory::createWindowOperator(windowDefinition);

    queryPlan->appendOperatorAsNewRoot(windowOperator);
    return *this;
}

}// namespace NES

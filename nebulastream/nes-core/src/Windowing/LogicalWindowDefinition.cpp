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

#include <API/Expressions/Expressions.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>

namespace NES::Windowing {

LogicalWindowDefinition::LogicalWindowDefinition(const std::vector<FieldAccessExpressionNodePtr> keys,
                                                 std::vector<WindowAggregationPtr> windowAggregation,
                                                 WindowTypePtr windowType,
                                                 DistributionCharacteristicPtr distChar,
                                                 WindowTriggerPolicyPtr triggerPolicy,
                                                 WindowActionDescriptorPtr triggerAction,
                                                 uint64_t allowedLateness)
    : windowAggregation(std::move(windowAggregation)), triggerPolicy(std::move(triggerPolicy)),
      triggerAction(std::move(triggerAction)), windowType(std::move(windowType)), onKey(std::move(keys)),
      distributionType(std::move(distChar)), allowedLateness(allowedLateness) {
    NES_TRACE("LogicalWindowDefinition: create new window definition");
}

bool LogicalWindowDefinition::isKeyed() { return !onKey.empty(); }

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(std::vector<WindowAggregationPtr> windowAggregations,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return create({}, windowAggregations, windowType, distChar, triggerPolicy, triggerAction, allowedLateness);
}

LogicalWindowDefinitionPtr LogicalWindowDefinition::create(std::vector<FieldAccessExpressionNodePtr> keys,
                                                           std::vector<WindowAggregationPtr> windowAggregation,
                                                           const WindowTypePtr& windowType,
                                                           const DistributionCharacteristicPtr& distChar,
                                                           const WindowTriggerPolicyPtr& triggerPolicy,
                                                           const WindowActionDescriptorPtr& triggerAction,
                                                           uint64_t allowedLateness) {
    return std::make_shared<LogicalWindowDefinition>(keys,
                                                     windowAggregation,
                                                     windowType,
                                                     distChar,
                                                     triggerPolicy,
                                                     triggerAction,
                                                     allowedLateness);
}

void LogicalWindowDefinition::setDistributionCharacteristic(DistributionCharacteristicPtr characteristic) {
    this->distributionType = std::move(characteristic);
}

DistributionCharacteristicPtr LogicalWindowDefinition::getDistributionType() { return distributionType; }
uint64_t LogicalWindowDefinition::getNumberOfInputEdges() const { return numberOfInputEdges; }
void LogicalWindowDefinition::setNumberOfInputEdges(uint64_t numberOfInputEdges) {
    this->numberOfInputEdges = numberOfInputEdges;
}
std::vector<WindowAggregationPtr> LogicalWindowDefinition::getWindowAggregation() { return windowAggregation; }
WindowTypePtr LogicalWindowDefinition::getWindowType() { return windowType; }
std::vector<FieldAccessExpressionNodePtr> LogicalWindowDefinition::getKeys() { return onKey; }
void LogicalWindowDefinition::setWindowAggregation(std::vector<WindowAggregationPtr> windowAggregation) {
    this->windowAggregation = std::move(windowAggregation);
}
void LogicalWindowDefinition::setWindowType(WindowTypePtr windowType) { this->windowType = std::move(windowType); }
void LogicalWindowDefinition::setOnKey(std::vector<FieldAccessExpressionNodePtr> onKey) { this->onKey = std::move(onKey); }

LogicalWindowDefinitionPtr LogicalWindowDefinition::copy() {
    return create(onKey, windowAggregation, windowType, distributionType, triggerPolicy, triggerAction, allowedLateness);
}
WindowTriggerPolicyPtr LogicalWindowDefinition::getTriggerPolicy() const { return triggerPolicy; }
void LogicalWindowDefinition::setTriggerPolicy(WindowTriggerPolicyPtr triggerPolicy) {
    this->triggerPolicy = std::move(triggerPolicy);
}

WindowActionDescriptorPtr LogicalWindowDefinition::getTriggerAction() const { return triggerAction; }

std::string LogicalWindowDefinition::toString() {
    std::stringstream ss;
    ss << std::endl;
    ss << "windowType=" << windowType->toString();
    //ss << " aggregation=" << windowAggregation->toString();
    ss << " triggerPolicy=" << triggerPolicy->toString() << std::endl;
    ss << " triggerAction=" << triggerAction->toString() << std::endl;
    if (isKeyed()) {
        //ss << " onKey=" << onKey << std::endl;
    }
    ss << " distributionType=" << distributionType->toString() << std::endl;
    ss << " numberOfInputEdges=" << numberOfInputEdges;
    ss << std::endl;
    return ss.str();
}
uint64_t LogicalWindowDefinition::getOriginId() const { return originId; }
void LogicalWindowDefinition::setOriginId(OriginId originId) { this->originId = originId; }
uint64_t LogicalWindowDefinition::getAllowedLateness() const { return allowedLateness; }

bool LogicalWindowDefinition::equal(LogicalWindowDefinitionPtr otherWindowDefinition) {

    if (this->isKeyed() != otherWindowDefinition->isKeyed()) {
        return false;
    }

    if (this->getKeys().size() != otherWindowDefinition->getKeys().size()) {
        return false;
    }

    for (uint64_t i = 0; i < this->getKeys().size(); i++) {
        if (!this->getKeys()[i]->equal(otherWindowDefinition->getKeys()[i])) {
            return false;
        };
    }

    if (this->getWindowAggregation().size() != otherWindowDefinition->getWindowAggregation().size()) {
        return false;
    }

    for (uint64_t i = 0; i < this->getWindowAggregation().size(); i++) {
        if (!this->getWindowAggregation()[i]->equal(otherWindowDefinition->getWindowAggregation()[i])) {
            return false;
        };
    }

    return this->windowType->equal(otherWindowDefinition->getWindowType());
}

}// namespace NES::Windowing
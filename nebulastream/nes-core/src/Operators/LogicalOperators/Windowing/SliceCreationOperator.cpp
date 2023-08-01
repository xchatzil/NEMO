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
#include <API/Schema.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>

namespace NES {

SliceCreationOperator::SliceCreationOperator(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, OperatorId id)
    : OperatorNode(id), WindowOperatorNode(windowDefinition, id) {
    this->windowDefinition->setDistributionCharacteristic(windowDefinition->getDistributionType());
    this->windowDefinition->setNumberOfInputEdges(windowDefinition->getNumberOfInputEdges());
    this->windowDefinition->setTriggerPolicy(windowDefinition->getTriggerPolicy());
    this->windowDefinition->setWindowAggregation(windowDefinition->getWindowAggregation());
    this->windowDefinition->setWindowType(windowDefinition->getWindowType());
    this->windowDefinition->setOnKey(windowDefinition->getKeys());
}

std::string SliceCreationOperator::toString() const {
    std::stringstream ss;
    ss << "SliceCreationOperator(" << id << ")";
    return ss.str();
}

bool SliceCreationOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<SliceCreationOperator>()->getId() == id;
}

bool SliceCreationOperator::equal(NodePtr const& rhs) const { return rhs->instanceOf<SliceCreationOperator>(); }

OperatorNodePtr SliceCreationOperator::copy() {
    auto copy = LogicalOperatorFactory::createSliceCreationSpecializedOperator(windowDefinition, id)->as<SliceCreationOperator>();
    copy->setOriginId(originId);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}
bool SliceCreationOperator::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    if (!WindowOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    // infer the default input and output schema
    NES_DEBUG("SliceCreationOperator: TypeInferencePhase: infer types for window operator with input schema "
              << inputSchema->toString());

    // infer type of aggregation
    auto windowAggregation = windowDefinition->getWindowAggregation();
    for (auto& agg : windowAggregation) {
        agg->inferStamp(typeInferencePhaseContext, inputSchema);
    }

    //Construct output schema
    outputSchema->clear();
    outputSchema =
        outputSchema
            ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "start", UINT64))
            ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "end", UINT64))
            ->addField(createField(inputSchema->getQualifierNameForSystemGeneratedFieldsWithSeparator() + "cnt", UINT64));

    if (windowDefinition->isKeyed()) {
        // infer the data type of the key field.
        auto keyList = windowDefinition->getKeys();
        for (auto& key : keyList) {
            key->inferStamp(typeInferencePhaseContext, inputSchema);
            outputSchema->addField(AttributeField::create(key->getFieldName(), key->getStamp()));
        }
    }
    for (auto& agg : windowAggregation) {
        outputSchema->addField(
            AttributeField::create(agg->as()->as<FieldAccessExpressionNode>()->getFieldName(), agg->on()->getStamp()));
    }
    return true;
}

void SliceCreationOperator::inferStringSignature() { NES_NOT_IMPLEMENTED(); }
}// namespace NES

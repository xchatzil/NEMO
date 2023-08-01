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
#include <API/Schema.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <utility>

namespace NES::Windowing {

MaxAggregationDescriptor::MaxAggregationDescriptor(FieldAccessExpressionNodePtr field)
    : WindowAggregationDescriptor(std::move(field)) {
    this->aggregationType = Max;
}

MaxAggregationDescriptor::MaxAggregationDescriptor(ExpressionNodePtr field, ExpressionNodePtr asField)
    : WindowAggregationDescriptor(std::move(field), std::move(asField)) {
    this->aggregationType = Max;
}

WindowAggregationPtr MaxAggregationDescriptor::create(FieldAccessExpressionNodePtr onField,
                                                      FieldAccessExpressionNodePtr asField) {
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(std::move(onField), std::move(asField)));
}

WindowAggregationPtr MaxAggregationDescriptor::on(ExpressionItem onField) {
    auto keyExpression = onField.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(fieldAccess));
}

void MaxAggregationDescriptor::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext,
                                          SchemaPtr schema) {
    // We first infer the stamp of the input field and set the output stamp as the same.
    onField->inferStamp(typeInferencePhaseContext, schema);
    if (!onField->getStamp()->isNumeric()) {
        NES_FATAL_ERROR("MaxAggregationDescriptor: aggregations on non numeric fields is not supported.");
    }

    //Set fully qualified name for the as Field
    auto onFieldName = onField->as<FieldAccessExpressionNode>()->getFieldName();
    auto asFieldName = asField->as<FieldAccessExpressionNode>()->getFieldName();

    auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    //If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos) {
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + asFieldName);
    } else {
        auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField->as<FieldAccessExpressionNode>()->updateFieldName(attributeNameResolver + fieldName);
    }
    asField->setStamp(onField->getStamp());
}

WindowAggregationPtr MaxAggregationDescriptor::copy() {
    return std::make_shared<MaxAggregationDescriptor>(MaxAggregationDescriptor(this->onField->copy(), this->asField->copy()));
}

DataTypePtr MaxAggregationDescriptor::getInputStamp() { return onField->getStamp(); }

DataTypePtr MaxAggregationDescriptor::getPartialAggregateStamp() { return onField->getStamp(); }

DataTypePtr MaxAggregationDescriptor::getFinalAggregateStamp() { return onField->getStamp(); }
}// namespace NES::Windowing
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
#include <API/Windowing.hpp>
#include <Nodes/Expressions/ExpressionNode.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
#include <utility>

namespace NES::Windowing {

TimeCharacteristic::TimeCharacteristic(Type type) : type(type), unit(API::Milliseconds()) {}
TimeCharacteristic::TimeCharacteristic(Type type, AttributeFieldPtr field, TimeUnit unit)
    : type(type), field(std::move(field)), unit(std::move(unit)) {}

TimeCharacteristicPtr TimeCharacteristic::createEventTime(ExpressionItem fieldValue, const TimeUnit& unit) {
    auto keyExpression = fieldValue.getExpressionNode();
    if (!keyExpression->instanceOf<FieldAccessExpressionNode>()) {
        NES_ERROR("Query: window key has to be an FieldAccessExpression but it was a " + keyExpression->toString());
    }
    auto fieldAccess = keyExpression->as<FieldAccessExpressionNode>();
    AttributeFieldPtr keyField = AttributeField::create(fieldAccess->getFieldName(), fieldAccess->getStamp());
    return std::make_shared<TimeCharacteristic>(Type::EventTime, keyField, unit);
}

TimeCharacteristicPtr TimeCharacteristic::createIngestionTime() {
    return std::make_shared<TimeCharacteristic>(Type::IngestionTime);
}

AttributeFieldPtr TimeCharacteristic::getField() { return field; }

TimeCharacteristic::Type TimeCharacteristic::getType() { return type; }

TimeUnit TimeCharacteristic::getTimeUnit() { return unit; }

std::string TimeCharacteristic::toString() {
    std::stringstream ss;
    ss << "TimeCharacteristic: ";
    ss << " type=" << getTypeAsString();
    if (field) {
        ss << " field=" << field->toString();
    }

    ss << std::endl;
    return ss.str();
}

std::string TimeCharacteristic::getTypeAsString() {
    switch (type) {
        case IngestionTime: return "IngestionTime";
        case EventTime: return "EventTime";
        default: return "Unknown TimeCharacteristic type";
    }
}

}// namespace NES::Windowing
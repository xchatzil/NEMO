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
#ifndef NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_EXPRESSIONWRAPPER_HPP_
#define NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_EXPRESSIONWRAPPER_HPP_

#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory>
namespace NES::Runtime::Execution::Expressions {

template<typename ExpressionType>
class UnaryExpressionWrapper {
  public:
    UnaryExpressionWrapper() {
        auto input = std::make_shared<ReadFieldExpression>("value");
        expression = std::make_shared<ExpressionType>(input);
    }
    Nautilus::Value<> eval(Nautilus::Value<> value) {
        auto record = Record({{"value", value}});
        return expression->execute(record);
    }

    std::shared_ptr<ExpressionType> expression;
};

template<typename ExpressionType>
class BinaryExpressionWrapper {
  public:
    BinaryExpressionWrapper() {
        auto leftExpression = std::make_shared<ReadFieldExpression>("left");
        auto rightExpression = std::make_shared<ReadFieldExpression>("right");
        expression = std::make_shared<ExpressionType>(leftExpression, rightExpression);
    }
    Nautilus::Value<> eval(Nautilus::Value<> left, Nautilus::Value<> right) {
        auto record = Record({{"left", left}, {"right", right}});
        return expression->execute(record);
    }

    std::shared_ptr<ExpressionType> expression;
};

}// namespace NES::Runtime::Execution::Expressions

#endif//NES_NES_RUNTIME_TESTS_INCLUDE_TESTUTILS_EXPRESSIONWRAPPER_HPP_

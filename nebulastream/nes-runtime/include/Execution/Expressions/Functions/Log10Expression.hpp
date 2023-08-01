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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_LOG10EXPRESSION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_LOG10EXPRESSION_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
     * @brief This expression cos the Expression
     */
class Log10Expression : public Expression {
  public:
    explicit Log10Expression(const ExpressionPtr& subExpression);
    Value<> execute(Record& record) const override;

  private:
    const ExpressionPtr subExpression;
};

}// namespace NES::Runtime::Execution::Expressions

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_FUNCTIONS_LOG10EXPRESSION_HPP_

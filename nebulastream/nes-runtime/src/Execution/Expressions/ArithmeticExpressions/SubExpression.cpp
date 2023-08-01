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
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>

namespace NES::Runtime::Execution::Expressions {

Value<> SubExpression::execute(Record& record) const {
    Value leftValue = leftSubExpression->execute(record);
    Value rightValue = rightSubExpression->execute(record);
    return leftValue - rightValue;
}
SubExpression::SubExpression(const ExpressionPtr& leftSubExpression, const ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

}// namespace NES::Runtime::Execution::Expressions
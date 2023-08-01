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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/TypeCastExprStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES::QueryCompilation {
StatementType TypeCastExprStatement::getStamentType() const { return TYPE_CAST_EXPR_STMT; }

CodeExpressionPtr TypeCastExprStatement::getCode() const {
    CodeExpressionPtr code;
    code = combine(std::make_shared<CodeExpression>("("), dataType->getCode());
    code = combine(code, std::make_shared<CodeExpression>(")"));
    code = combine(
        code,
        combine(combine(std::make_shared<CodeExpression>("("), expression->getCode()), std::make_shared<CodeExpression>(")")));
    return code;
}

ExpressionStatementPtr TypeCastExprStatement::copy() const { return std::make_shared<TypeCastExprStatement>(*this); }

TypeCastExprStatement::TypeCastExprStatement(const ExpressionStatement& expr, GeneratableDataTypePtr type)
    : expression(expr.copy()), dataType(std::move(type)) {}

TypeCastExprStatement::~TypeCastExprStatement() = default;
}// namespace NES::QueryCompilation

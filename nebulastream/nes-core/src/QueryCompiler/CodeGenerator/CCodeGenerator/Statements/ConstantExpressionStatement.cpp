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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ConstantExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableValueType.hpp>
#include <utility>

namespace NES::QueryCompilation {

StatementType ConstantExpressionStatement::getStamentType() const { return CONSTANT_VALUE_EXPR_STMT; }

CodeExpressionPtr ConstantExpressionStatement::getCode() const { return constantValue->getCodeExpression(); }

ExpressionStatementPtr ConstantExpressionStatement::copy() const {
    auto copy = std::make_shared<ConstantExpressionStatement>(*this);
    copy->constantValue = constantValue;
    return copy;
}

ConstantExpressionStatement::ConstantExpressionStatement(GeneratableValueTypePtr val) : constantValue(std::move(val)) {}
}// namespace NES::QueryCompilation
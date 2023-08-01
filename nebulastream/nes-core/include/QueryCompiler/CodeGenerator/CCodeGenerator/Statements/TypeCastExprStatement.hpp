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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TYPECASTEXPRSTATEMENT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TYPECASTEXPRSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>

namespace NES::QueryCompilation {

class TypeCastExprStatement : public ExpressionStatement {
  public:
    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] ExpressionStatementPtr copy() const override;

    TypeCastExprStatement(const TypeCastExprStatement&) = default;

    TypeCastExprStatement(const ExpressionStatement& expr, GeneratableDataTypePtr type);

    ~TypeCastExprStatement() override;

  private:
    ExpressionStatementPtr expression;
    GeneratableDataTypePtr dataType;
};

using TypeCast = TypeCastExprStatement;

}// namespace NES::QueryCompilation

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_TYPECASTEXPRSTATEMENT_HPP_

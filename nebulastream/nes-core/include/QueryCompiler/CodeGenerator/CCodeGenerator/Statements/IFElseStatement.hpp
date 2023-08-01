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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_IFELSESTATEMENT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_IFELSESTATEMENT_HPP_
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
namespace NES {
namespace QueryCompilation {
class IfElseStatement : public Statement {
  public:
    explicit IfElseStatement(const Statement& condExpr);
    IfElseStatement(const Statement& condExpr, const Statement& condTrueStmt, const Statement& condFalseStmt);

    [[nodiscard]] StatementType getStamentType() const override;
    [[nodiscard]] CodeExpressionPtr getCode() const override;
    [[nodiscard]] StatementPtr createCopy() const override;

    CompoundStatementPtr getTrueCaseCompoundStatement();
    CompoundStatementPtr getFalseCaseCompoundStatement();

    ~IfElseStatement() noexcept override = default;

  private:
    const StatementPtr conditionalExpression;
    CompoundStatementPtr trueCaseStatement;
    CompoundStatementPtr falseCaseStatement;
};

using IFELSE = IfElseStatement;
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_IFELSESTATEMENT_HPP_

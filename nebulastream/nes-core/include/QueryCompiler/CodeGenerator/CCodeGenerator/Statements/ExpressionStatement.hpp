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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_EXPRESSIONSTATEMENT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_EXPRESSIONSTATEMENT_HPP_
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES::QueryCompilation {

class ExpressionStatement : public Statement {
  public:
    [[nodiscard]] StatementType getStamentType() const override = 0;
    [[nodiscard]] CodeExpressionPtr getCode() const override = 0;
    /** @brief virtual copy constructor */
    [[nodiscard]] virtual ExpressionStatementPtr copy() const = 0;
    /** @brief virtual copy constructor of base class
     *  todo create one unified version, having this twice is problematic
    */
    [[nodiscard]] StatementPtr createCopy() const final;

    BinaryOperatorStatement assign(const ExpressionStatement& ref);
    [[nodiscard]] BinaryOperatorStatement assign(ExpressionStatementPtr ref) const;
    BinaryOperatorStatement accessPtr(const ExpressionStatement& ref);
    [[nodiscard]] BinaryOperatorStatement accessPtr(ExpressionStatementPtr const& ref) const;
    BinaryOperatorStatement accessRef(const ExpressionStatement& ref);
    [[nodiscard]] BinaryOperatorStatement accessRef(ExpressionStatementPtr ref) const;
    BinaryOperatorStatement operator[](const ExpressionStatement& ref);

    ExpressionStatement() = default;
    ExpressionStatement(const ExpressionStatement&) = default;
    ~ExpressionStatement() override = default;
};

}// namespace NES::QueryCompilation
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_EXPRESSIONSTATEMENT_HPP_

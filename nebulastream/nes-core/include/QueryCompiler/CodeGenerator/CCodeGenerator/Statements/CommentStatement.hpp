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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_COMMENTSTATEMENT_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_COMMENTSTATEMENT_HPP_

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>

namespace NES::QueryCompilation {
/**
 * @brief this statements allows us to generate code comments
 */
class CommentStatement : public Statement {
  public:
    explicit inline CommentStatement(const std::string& comment) noexcept : comment(comment) {}
    explicit inline CommentStatement(std::string&& comment) noexcept : comment(std::move(comment)) {}

    ~CommentStatement() noexcept = default;

    [[nodiscard]] StatementType getStamentType() const override;

    [[nodiscard]] CodeExpressionPtr getCode() const override;

    [[nodiscard]] StatementPtr createCopy() const override;

  private:
    std::string comment;
};

using Comment = CommentStatement;

}// namespace NES::QueryCompilation

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_STATEMENTS_COMMENTSTATEMENT_HPP_

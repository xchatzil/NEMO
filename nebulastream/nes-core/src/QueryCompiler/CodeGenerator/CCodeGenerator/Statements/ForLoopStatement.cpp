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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/CompoundStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ExpressionStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/ForLoopStatement.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <QueryCompiler/CodeGenerator/CodeExpression.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace NES::QueryCompilation {
ForLoopStatement::ForLoopStatement(DeclarationPtr varDeclaration,
                                   ExpressionStatementPtr condition,
                                   ExpressionStatementPtr advance,
                                   std::vector<StatementPtr> loopBody)
    : varDeclaration(std::move(varDeclaration)), varDeclarationStatement(nullptr), condition(std::move(condition)),
      advance(std::move(advance)), body(std::make_shared<CompoundStatement>()) {
    for (auto const& stmt : loopBody) {
        if (stmt) {
            body->addStatement(stmt);
        }
    }
}
ForLoopStatement::ForLoopStatement(ExpressionStatementPtr varDeclarationStatement,
                                   ExpressionStatementPtr condition,
                                   ExpressionStatementPtr advance,
                                   std::vector<StatementPtr> loopBody)
    : varDeclaration(nullptr), varDeclarationStatement(std::move(varDeclarationStatement)), condition(std::move(condition)),
      advance(std::move(advance)), body(std::make_shared<CompoundStatement>()) {
    for (auto const& stmt : loopBody) {
        if (stmt) {
            body->addStatement(stmt);
        }
    }
}

ForLoopStatement::ForLoopStatement(ExpressionStatementPtr condition,
                                   ExpressionStatementPtr advance,
                                   std::vector<StatementPtr> loopBody)
    : varDeclaration(nullptr), varDeclarationStatement(nullptr), condition(std::move(condition)), advance(std::move(advance)),
      body(std::make_shared<CompoundStatement>()) {
    for (auto const& stmt : loopBody) {
        if (stmt) {
            body->addStatement(stmt);
        }
    }
}

StatementPtr ForLoopStatement::createCopy() const { return std::make_shared<ForLoopStatement>(*this); }

CompoundStatementPtr ForLoopStatement::getCompoundStatement() { return body; }

StatementType ForLoopStatement::getStamentType() const { return StatementType::FOR_LOOP_STMT; }

CodeExpressionPtr ForLoopStatement::getCode() const {
    if (varDeclaration && varDeclarationStatement) {
        NES_ERROR("ForLoopStatement: At most one of varDeclaration & varDeclarationStatement"
                  "should be defined for code generation.");
    }

    std::stringstream code;
    code << "for(";
    if (varDeclaration) {
        code << varDeclaration->getCode() << ";";
    } else if (varDeclarationStatement) {
        code << varDeclarationStatement->getCode()->code_ << ";";
    }
    code << condition->getCode()->code_ << ";" << advance->getCode()->code_ << "){" << std::endl;
    code << body->getCode()->code_ << std::endl;
    code << "}" << std::endl;
    return std::make_shared<CodeExpression>(code.str());
}

void ForLoopStatement::addStatement(StatementPtr const& stmt) {
    if (stmt) {
        body->addStatement(stmt);
    }
}

}// namespace NES::QueryCompilation

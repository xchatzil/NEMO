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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/FunctionDeclaration.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <utility>

namespace NES::QueryCompilation {
FunctionDeclaration::FunctionDeclaration(Code code) : functionCode(std::move(code)) {}

FunctionDeclarationPtr FunctionDeclaration::create(const Code& code) { return std::make_shared<FunctionDeclaration>(code); }

GeneratableDataTypePtr FunctionDeclaration::getType() const { return GeneratableDataTypePtr(); }
std::string FunctionDeclaration::getIdentifierName() const { return ""; }

Code FunctionDeclaration::getTypeDefinitionCode() const { return Code(); }

Code FunctionDeclaration::getCode() const { return functionCode; }
DeclarationPtr FunctionDeclaration::copy() const { return std::make_shared<FunctionDeclaration>(*this); }
}// namespace NES::QueryCompilation
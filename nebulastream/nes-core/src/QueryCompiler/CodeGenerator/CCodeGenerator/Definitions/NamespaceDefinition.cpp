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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/NamespaceDeclaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Definitions/NamespaceDefinition.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>
namespace NES::QueryCompilation {
NamespaceDefinition::NamespaceDefinition(std::string name) : name(std::move(name)) {}

NamespaceDefinitionPtr NamespaceDefinition::create(const std::string& name) {
    return std::make_shared<NamespaceDefinition>(name);
}

void NamespaceDefinition::addDeclaration(const DeclarationPtr& declaration) { this->declarations.emplace_back(declaration); }

DeclarationPtr NamespaceDefinition::getDeclaration() {
    std::stringstream namespaceCode;
    namespaceCode << "namespace " << name;
    namespaceCode << "{";
    for (const auto& declaration : declarations) {
        namespaceCode << declaration->getCode();
    }
    namespaceCode << "}";

    return NamespaceDeclaration::create(namespaceCode.str());
}
}// namespace NES::QueryCompilation

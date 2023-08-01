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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DEFINITIONS_NAMESPACEDEFINITION_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DEFINITIONS_NAMESPACEDEFINITION_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <string>
#include <vector>

namespace NES {
namespace QueryCompilation {
/**
 * @brief Defines a namespace scope
 * For example:
 * namespace NES{
 *
 * ..... all declarations added to this namespaces.
 *
 * }
 */
class NamespaceDefinition {
  public:
    static NamespaceDefinitionPtr create(const std::string& name);
    explicit NamespaceDefinition(std::string name);
    /**
     * @brief Adds a new declaration to this namespace
     * @param declaration
     */
    void addDeclaration(const DeclarationPtr& declaration);

    DeclarationPtr getDeclaration();

  private:
    std::string name;
    std::vector<DeclarationPtr> declarations;
};
}// namespace QueryCompilation
}// namespace NES
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_CCODEGENERATOR_DEFINITIONS_NAMESPACEDEFINITION_HPP_

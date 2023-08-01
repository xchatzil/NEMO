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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_TRANSLATETOLEGACYEXPRESSION_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_TRANSLATETOLEGACYEXPRESSION_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>

namespace NES {

namespace QueryCompilation {

/**
 * @brief Translates a logical query plan to the legacy operator tree
 */
class TranslateToLegacyExpression {
  public:
    /**
     * @brief Factory method to create a translator phase.
     */
    static TranslateToLegacyExpressionPtr create();

    TranslateToLegacyExpression();

    /**
     * @brief Translates an expression to a legacy user api expression.
     * @param expression node
     * @return LegacyExpressionPtr
     */
    LegacyExpressionPtr transformExpression(const ExpressionNodePtr& expression);

    /**
     * @brief Translates logical expessions to a legacy user api expression.
     * @param expression node
     * @return LegacyExpressionPtr
     */
    LegacyExpressionPtr transformLogicalExpressions(const ExpressionNodePtr& expression);

    /**
     * @brief Translates arithmetical expessions to a legacy user api expression.
     * @param expression node
     * @return LegacyExpressionPtr
     */
    LegacyExpressionPtr transformArithmeticalExpressions(const ExpressionNodePtr& expression);
};
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_CODEGENERATOR_TRANSLATETOLEGACYEXPRESSION_HPP_

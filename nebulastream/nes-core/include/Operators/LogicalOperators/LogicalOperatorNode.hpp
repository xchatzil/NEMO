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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORNODE_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/OperatorNode.hpp>

namespace z3 {
class expr;
using ExprPtr = std::shared_ptr<expr>;
class context;
using ContextPtr = std::shared_ptr<context>;
}// namespace z3

namespace NES::Optimizer {
class TypeInferencePhaseContext;
class QuerySignature;
using QuerySignaturePtr = std::shared_ptr<QuerySignature>;
}// namespace NES::Optimizer

namespace NES {

/**
 * @brief Logical operator, enables schema inference and signature computation.
 */
class LogicalOperatorNode : public virtual OperatorNode {

  public:
    explicit LogicalOperatorNode(OperatorId id);

    /**
     * @brief Get the First Order Logic formula representation by the Z3 expression
     * @param context: the shared pointer to the z3::context
     */
    void inferZ3Signature(const z3::ContextPtr& context);

    /**
     * @brief Set the Z3 signature for the logical operator
     * @param signature : the signature
     */
    void setZ3Signature(Optimizer::QuerySignaturePtr signature);

    /**
     * @brief Infers the input origin of a logical operator.
     * If this operator dose not assign new origin ids, e.g., windowing,
     * this function collects the origin ids from all upstream operators.
     */
    virtual void inferInputOrigins() = 0;

    /**
     * @brief Get the String based signature for the operator
     */
    virtual void inferStringSignature() = 0;

    /**
     * @brief Set the hash based signature for the logical operator
     * @param signature : the signature
     */
    void setHashBasedSignature(std::map<size_t, std::set<std::string>> signature);

    /**
     * @brief update the hash based signature for the logical operator
     * @param signature : the signature
     */
    void updateHashBasedSignature(size_t hashCode, std::string stringSignature);

    /**
     * @brief Get the Z3 expression for the logical operator
     * @return reference to the Z3 expression
     */
    Optimizer::QuerySignaturePtr getZ3Signature();

    /**
     * @brief Get the string signature computed based on upstream operator chain
     * @return string representing the query signature
     */
    std::map<size_t, std::set<std::string>> getHashBasedSignature();

    /**
     * @brief infers the input and out schema of this operator depending on its child.
     * @param typeInferencePhaseContext needed for stamp inferring
     * @return true if schema was correctly inferred
     */
    virtual bool inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) = 0;

  protected:
    Optimizer::QuerySignaturePtr z3Signature;
    std::map<size_t, std::set<std::string>> hashBasedSignature;
    std::hash<std::string> hashGenerator;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_LOGICALOPERATORNODE_HPP_

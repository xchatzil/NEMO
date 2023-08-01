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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical Scan operator.
 */
class PhysicalScanOperator : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    /**
     * @brief Constructor for the physical scan operator
     * @param id operator id
     * @param outputSchema output schema
     */
    PhysicalScanOperator(OperatorId id, const SchemaPtr& outputSchema);

    /**
     * @brief Creates for the physical scan operator
     * @param id operator id
     * @param outputSchema output schema
     */
    static PhysicalOperatorPtr create(OperatorId id, const SchemaPtr& outputSchema);

    /**
     * @brief Constructor for the physical scan operator
     * @param outputSchema output schema
     */
    static PhysicalOperatorPtr create(SchemaPtr outputSchema);
    std::string toString() const override;
    OperatorNodePtr copy() override;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALSCANOPERATOR_HPP_

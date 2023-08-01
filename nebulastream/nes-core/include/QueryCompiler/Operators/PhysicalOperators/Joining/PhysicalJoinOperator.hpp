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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalBinaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {
/**
 * @brief Physical operator for the join build.
 * This operator receives input records and adds them to its operator state.
 */
class PhysicalJoinOperator {
  public:
    Join::JoinOperatorHandlerPtr getJoinHandler();

    virtual ~PhysicalJoinOperator() noexcept = default;

  protected:
    explicit PhysicalJoinOperator(Join::JoinOperatorHandlerPtr operatorHandler);

    Join::JoinOperatorHandlerPtr operatorHandler;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_JOINING_PHYSICALJOINOPERATOR_HPP_

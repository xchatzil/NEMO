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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXECUTABLEOPERATOR_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXECUTABLEOPERATOR_HPP_
#include <Execution/Operators/Operator.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief Base class of executable operators, which receive tuple by tuple.
 * Within a pipeline all operators except the initial scan are executable operators.
 */
class ExecutableOperator : public Operator {
  public:
    /**
     * @brief This method is called by the upstream operator (parent) and passes one record for execution.
     * @param ctx the execution context that allows accesses to local and global state.
     * @param record the record that should be processed.
     */
    virtual void execute(ExecutionContext& ctx, Record& record) const = 0;
    virtual ~ExecutableOperator() = default;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_EXECUTABLEOPERATOR_HPP_

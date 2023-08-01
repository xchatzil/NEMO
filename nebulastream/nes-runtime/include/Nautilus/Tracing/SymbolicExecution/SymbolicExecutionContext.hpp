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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionPath.hpp>
#include <Nautilus/Tracing/Tag/Tag.hpp>
#include <list>
#include <unordered_map>
namespace NES::Nautilus::Tracing {
class TagRecorder;

/**
 * @brief The symbolic execution context supports the symbolic execution of functions.
 * In general it executes a function with dummy parameters and explores all possible execution paths.
 */
class SymbolicExecutionContext {
  public:
    // The number of iterations we want to spend maximally to explore executions.
    static const uint64_t MAX_ITERATIONS = 100000;
    /**
     * @brief Performs a symbolic execution of a CMP operation.
     * Depending on all previous executions this function determines if a branch should be explored or not.
     * @return the return value of this branch
     */
    bool executeCMP(TagRecorder& tr);
    /**
     * @brief Check if we should continue the symbolic execution or if we evaluated all possible execution passes.
     * @return false if all execution passes trough a function have been evaluated.
     */
    bool shouldContinue();
    /**
     * @brief Initializes the next iteration of the symbolic execution.
     */
    void next();
    /**
     * @brief Returns the number of iterations of symbolic execution.
     * @return uint64_t
     */
    uint64_t getIterations() const;

  private:
    /**
     * @brief Records a new cmp operation
     * @param tr TagRecorder
     * @return
     */
    bool record(TagRecorder& tr);

  private:
    /**
     * @brief Symbolic execution mode.
     * That identifies if, we follow a previously recorded execution or if we record a new one.
     */
    enum MODE : const int8_t { FOLLOW, RECORD };
    /**
     * @brief Tag state
     * This indicates if we visited a specific tag one or two times.
     * If we already visited it two times, we can skip any further executions at this point.
     */
    enum TagState : const int8_t { FirstVisit, SecondVisit };
    std::unordered_map<const Tag*, TagState> tagMap;
    std::list<SymbolicExecutionPath> inflightExecutionPaths;
    MODE currentMode = RECORD;
    SymbolicExecutionPath currentExecutionPath = SymbolicExecutionPath();
    uint64_t currentOperation = 0;
    uint64_t iterations = 0;
};

}// namespace NES::Nautilus::Tracing

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_SYMBOLICEXECUTION_SYMBOLICEXECUTIONCONTEXT2_HPP_

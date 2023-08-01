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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLIDINGWINDOWSINK_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLIDINGWINDOWSINK_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief The keyed slicing window sink uses the global slice store to compute the final aggregate for a sliding window.
 */
class PhysicalKeyedSlidingWindowSink : public PhysicalUnaryOperator, public AbstractScanOperator {
  public:
    PhysicalKeyedSlidingWindowSink(OperatorId id,
                                   SchemaPtr inputSchema,
                                   SchemaPtr outputSchema,
                                   Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler);

    static std::shared_ptr<PhysicalKeyedSlidingWindowSink>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler);

    std::string toString() const override;
    OperatorNodePtr copy() override;

    Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandlerPtr getWindowHandler() { return keyedEventTimeWindowHandler; }

  private:
    Windowing::Experimental::KeyedSlidingWindowSinkOperatorHandlerPtr keyedEventTimeWindowHandler;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDSLIDINGWINDOWSINK_HPP_

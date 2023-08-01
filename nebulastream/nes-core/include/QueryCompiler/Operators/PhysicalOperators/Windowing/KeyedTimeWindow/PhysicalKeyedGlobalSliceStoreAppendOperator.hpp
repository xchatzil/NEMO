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

#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_
#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>
namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Appends the final slices to the global slice store.
 */
class PhysicalKeyedGlobalSliceStoreAppendOperator : public PhysicalUnaryOperator, public AbstractEmitOperator {
  public:
    PhysicalKeyedGlobalSliceStoreAppendOperator(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler);

    static std::shared_ptr<PhysicalKeyedGlobalSliceStoreAppendOperator>
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandlerPtr getWindowHandler() {
        return keyedEventTimeWindowHandler;
    }

  private:
    Windowing::Experimental::KeyedGlobalSliceStoreAppendOperatorHandlerPtr keyedEventTimeWindowHandler;
};

}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_WINDOWING_KEYEDTIMEWINDOW_PHYSICALKEYEDGLOBALSLICESTOREAPPENDOPERATOR_HPP_

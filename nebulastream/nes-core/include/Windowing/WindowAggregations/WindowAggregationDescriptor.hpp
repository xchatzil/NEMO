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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_

#include <Common/DataTypes/DataType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Optimizer {
class TypeInferencePhaseContext;
}

namespace NES::Windowing {
/**
 * Abstract class for window aggregations. All window aggregations operate on a field and output another field.
 */
class WindowAggregationDescriptor {
  public:
    enum Type { Avg, Count, Max, Min, Sum, Median };

    /**
    * Defines the field to which a aggregate output is assigned.
    * @param asField
    * @return WindowAggregationDescriptor
    */
    WindowAggregationDescriptorPtr as(const ExpressionItem& asField);

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr as();

    /**
    * Returns the result field of the aggregation
    * @return ExpressionNodePtr
    */
    ExpressionNodePtr on();

    /**
     * @brief Returns the type of this aggregation.
     * @return WindowAggregationDescriptor::Type
     */
    Type getType();

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    virtual void inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) = 0;

    /**
    * @brief Creates a deep copy of the window aggregation
    */
    virtual WindowAggregationPtr copy() = 0;

    /**
     * @return the input type
     */
    virtual DataTypePtr getInputStamp() = 0;

    /**
     * @return the partial aggregation type
     */
    virtual DataTypePtr getPartialAggregateStamp() = 0;

    /**
     * @return the final aggregation type
     */
    virtual DataTypePtr getFinalAggregateStamp() = 0;

    std::string toString();

    std::string getTypeAsString();

    /**
     * @brief Check if input window aggregation is equal to this window aggregation definition by checking the aggregation type,
     * on field, and as field
     * @param otherWindowAggregationDescriptor : other window aggregation definition
     * @return true if equal else false
     */
    bool equal(WindowAggregationDescriptorPtr otherWindowAggregationDescriptor);

  protected:
    explicit WindowAggregationDescriptor(const FieldAccessExpressionNodePtr& onField);
    WindowAggregationDescriptor(const ExpressionNodePtr& onField, const ExpressionNodePtr& asField);
    WindowAggregationDescriptor() = default;
    ExpressionNodePtr onField;
    ExpressionNodePtr asField;
    Type aggregationType;
};
}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_WINDOWAGGREGATIONDESCRIPTOR_HPP_

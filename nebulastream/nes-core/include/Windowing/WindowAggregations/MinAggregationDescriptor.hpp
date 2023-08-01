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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MINAGGREGATIONDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MINAGGREGATIONDESCRIPTOR_HPP_

#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
namespace NES::Windowing {

/**
 * @brief
 * The MinAggregationDescriptor aggregation calculates the minimum over the window.
 */
class MinAggregationDescriptor : public WindowAggregationDescriptor {
  public:
    /**
   * Factory method to creates a MinAggregationDescriptor aggregation on a particular field.
   */
    static WindowAggregationPtr on(ExpressionItem onField);

    static WindowAggregationPtr create(FieldAccessExpressionNodePtr onField, FieldAccessExpressionNodePtr asField);

    /**
     * @brief Infers the stamp of the expression given the current schema and the typeInferencePhaseContext.
     * @param typeInferencePhaseContext
     * @param schema
     */
    void inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) override;
    WindowAggregationPtr copy() override;
    DataTypePtr getInputStamp() override;
    DataTypePtr getPartialAggregateStamp() override;
    DataTypePtr getFinalAggregateStamp() override;

    virtual ~MinAggregationDescriptor() = default;

  private:
    explicit MinAggregationDescriptor(FieldAccessExpressionNodePtr onField);
    MinAggregationDescriptor(ExpressionNodePtr onField, ExpressionNodePtr asField);
};
}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWAGGREGATIONS_MINAGGREGATIONDESCRIPTOR_HPP_

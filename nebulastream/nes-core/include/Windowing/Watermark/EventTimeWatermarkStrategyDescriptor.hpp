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

#ifndef NES_CORE_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_

#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowMeasures/TimeUnit.hpp>
namespace NES::Windowing {

class EventTimeWatermarkStrategyDescriptor;
using EventTimeWatermarkStrategyDescriptorPtr = std::shared_ptr<EventTimeWatermarkStrategyDescriptor>;

class EventTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create(const ExpressionItem& onField, TimeMeasure allowedLateness, TimeUnit unit);

    ExpressionNodePtr getOnField();

    TimeMeasure getAllowedLateness();

    TimeUnit getTimeUnit();

    bool equal(WatermarkStrategyDescriptorPtr other) override;

    std::string toString() override;

    bool inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) override;

  private:
    // Field where the watermark should be retrieved
    ExpressionNodePtr onField;
    TimeUnit unit;
    TimeMeasure allowedLateness;

    explicit EventTimeWatermarkStrategyDescriptor(const ExpressionItem& onField, TimeMeasure allowedLateness, TimeUnit unit);
};

}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WATERMARK_EVENTTIMEWATERMARKSTRATEGYDESCRIPTOR_HPP_

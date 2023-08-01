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

#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <utility>

namespace NES::Windowing {

EventTimeWatermarkStrategy::EventTimeWatermarkStrategy(FieldAccessExpressionNodePtr onField,
                                                       uint64_t allowedLateness,
                                                       uint64_t multiplier)
    : onField(std::move(onField)), multiplier(multiplier), allowedLateness(allowedLateness) {}

FieldAccessExpressionNodePtr NES::Windowing::EventTimeWatermarkStrategy::getField() { return onField; }
uint64_t EventTimeWatermarkStrategy::getAllowedLateness() const { return allowedLateness; }

WatermarkStrategy::Type EventTimeWatermarkStrategy::getType() { return WatermarkStrategy::EventTimeWatermark; }

EventTimeWatermarkStrategyPtr
EventTimeWatermarkStrategy::create(const FieldAccessExpressionNodePtr& onField, uint64_t allowedLateness, uint64_t multiplier) {
    return std::make_shared<Windowing::EventTimeWatermarkStrategy>(onField, allowedLateness, multiplier);
}
uint64_t EventTimeWatermarkStrategy::getMultiplier() const { return multiplier; }

}// namespace NES::Windowing

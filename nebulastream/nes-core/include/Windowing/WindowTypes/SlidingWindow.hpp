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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
namespace NES::Windowing {
/**
 * A SlidingWindow assigns records to multiple overlapping windows.
 */
class SlidingWindow : public TimeBasedWindowType {
  public:
    static WindowTypePtr of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide);

    /**
    * Calculates the next window end based on a given timestamp.
    * @param currentTs
    * @return the next window end
    */
    [[nodiscard]] uint64_t calculateNextWindowEnd(uint64_t currentTs) const override;

    /**
     * @brief Generates and adds all windows between which ended size the last watermark till the current watermark.
     * @param windows vector of windows
     * @param lastWatermark
     * @param currentWatermark
     */
    void triggerWindows(std::vector<WindowState>& windows, uint64_t lastWatermark, uint64_t currentWatermark) const override;

    /**
     * @brief Returns true, because this a a sliding window
     * @return true
     */
    bool isSlidingWindow() override;

    /**
    * @brief return size of the window
    * @return size of the window
    */
    TimeMeasure getSize() override;

    /**
    * @brief return size of the slide
    * @return size of the slide
    */
    TimeMeasure getSlide() override;

    std::string toString() override;

    bool equal(WindowTypePtr otherWindowType) override;

  private:
    SlidingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size, TimeMeasure slide);
    const TimeMeasure size;
    const TimeMeasure slide;
};

}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_SLIDINGWINDOW_HPP_

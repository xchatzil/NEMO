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

#ifndef NES_CORE_INCLUDE_WINDOWING_WATERMARK_INGESTIONTIMEWATERMARKGENERATOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WATERMARK_INGESTIONTIMEWATERMARKGENERATOR_HPP_
#include <Windowing/Watermark/Watermark.hpp>
#include <cstdint>
#include <memory>

namespace NES::Windowing {

class IngestionTimeWatermarkGenerator : public Watermark {
  public:
    explicit IngestionTimeWatermarkGenerator();

    /**
    * @brief this function returns the watermark value as a processing time value
    * @return watermark value
    */
    uint64_t getWatermark() override;
};

}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_WATERMARK_INGESTIONTIMEWATERMARKGENERATOR_HPP_

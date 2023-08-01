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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_CONTENTBASEDWINDOWTYPE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_CONTENTBASEDWINDOWTYPE_HPP_

#include <Windowing/WindowTypes/WindowType.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <vector>

namespace NES::Windowing {
class ContentBasedWindowType : public WindowType {
  public:
    explicit ContentBasedWindowType();

    virtual ~ContentBasedWindowType() = default;

    /**
     * @brief Infer stamp of content-based window type
     * @param schema : the schema of the window
     * @return true if success else false
     */
    bool inferStamp(const SchemaPtr& schema) override;

    /**
       * Cast the current window type as a threshold window type
       * @return a shared pointer of ThresholdWindow
       */
    static ThresholdWindowPtr asThresholdWindow(ContentBasedWindowTypePtr contentBasedWindowType);
};
}// namespace NES::Windowing

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWTYPES_CONTENTBASEDWINDOWTYPE_HPP_

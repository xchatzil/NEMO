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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEWINDOWACTIONDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEWINDOWACTIONDESCRIPTOR_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

enum ActionType { WindowAggregationTriggerAction, SliceAggregationTriggerAction, SliceCombinerTriggerAction };

class BaseWindowActionDescriptor {
  public:
    virtual ~BaseWindowActionDescriptor() noexcept = default;
    /**
     * @brief this function will return the type of the action
     * @return
     */
    virtual ActionType getActionType() = 0;

    std::string toString();

    std::string getTypeAsString();

  protected:
    explicit BaseWindowActionDescriptor(ActionType action);
    ActionType action;
};

}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWACTIONS_BASEWINDOWACTIONDESCRIPTOR_HPP_

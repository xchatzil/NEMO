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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEWINDOWTRIGGERPOLICYDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEWINDOWTRIGGERPOLICYDESCRIPTOR_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

enum TriggerType { triggerOnTime, triggerOnRecord, triggerOnBuffer, triggerOnWatermarkChange };

class BaseWindowTriggerPolicyDescriptor {
  public:
    /**
     * @brief this function will return the type of the policy
     * @return
     */
    virtual TriggerType getPolicyType() = 0;

    virtual std::string toString() = 0;

    virtual ~BaseWindowTriggerPolicyDescriptor() = default;

    std::string getTypeAsString();

  protected:
    explicit BaseWindowTriggerPolicyDescriptor(TriggerType policy);
    TriggerType policy;
};

}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_BASEWINDOWTRIGGERPOLICYDESCRIPTOR_HPP_

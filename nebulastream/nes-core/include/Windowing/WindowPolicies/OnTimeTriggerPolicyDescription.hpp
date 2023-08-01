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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_ONTIMETRIGGERPOLICYDESCRIPTION_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_ONTIMETRIGGERPOLICYDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnTimeTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create(uint64_t triggerTimeInMs);

    ~OnTimeTriggerPolicyDescription() noexcept override = default;

    /**
     * @brief method to get the policy type
     * @return
     */
    TriggerType getPolicyType() override;

    /**
    * @brief getter for the time in ms
    * @return time in ms between two triggers
    */
    [[nodiscard]] uint64_t getTriggerTimeInMs() const;

    std::string toString() override;

  protected:
    explicit OnTimeTriggerPolicyDescription(uint64_t triggerTimeInMs);
    uint64_t triggerTimeInMs;
};

using OnTimeTriggerDescriptionPtr = std::shared_ptr<OnTimeTriggerPolicyDescription>;
}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWPOLICIES_ONTIMETRIGGERPOLICYDESCRIPTION_HPP_

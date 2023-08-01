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

#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <sstream>
namespace NES::Windowing {

WindowTriggerPolicyPtr OnTimeTriggerPolicyDescription::create(uint64_t triggerTimeInMs) {
    return std::make_shared<OnTimeTriggerPolicyDescription>(OnTimeTriggerPolicyDescription(triggerTimeInMs));
}
TriggerType OnTimeTriggerPolicyDescription::getPolicyType() { return this->policy; }

uint64_t OnTimeTriggerPolicyDescription::getTriggerTimeInMs() const { return triggerTimeInMs; }

OnTimeTriggerPolicyDescription::OnTimeTriggerPolicyDescription(uint64_t triggerTimeInMs)
    : BaseWindowTriggerPolicyDescriptor(triggerOnTime), triggerTimeInMs(triggerTimeInMs) {}

std::string OnTimeTriggerPolicyDescription::toString() {
    std::stringstream ss;
    ss << getTypeAsString() << "triggerTimeInMs=" << triggerTimeInMs;
    return ss.str();
}

}// namespace NES::Windowing
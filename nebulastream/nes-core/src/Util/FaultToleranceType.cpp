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
#include <Util/FaultToleranceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

namespace NES {
FaultToleranceType::Value FaultToleranceType::getFromString(const std::string faultToleranceMode) {
    if (faultToleranceMode == "NONE") {
        return FaultToleranceType::NONE;
    } else if (faultToleranceMode == "AT_MOST_ONCE") {
        return FaultToleranceType::AT_MOST_ONCE;
    } else if (faultToleranceMode == "AT_LEAST_ONCE") {
        return FaultToleranceType::AT_LEAST_ONCE;
    } else if (faultToleranceMode == "EXACTLY_ONCE") {
        return FaultToleranceType::EXACTLY_ONCE;
    } else {
        NES_THROW_RUNTIME_ERROR("FaultToleranceType not supported " + faultToleranceMode);
    }
}

std::string FaultToleranceType::toString(const Value faultToleranceMode) {
    switch (faultToleranceMode) {
        case FaultToleranceType::NONE: return "NONE";
        case FaultToleranceType::AT_MOST_ONCE: return "AT_MOST_ONCE";
        case FaultToleranceType::AT_LEAST_ONCE: return "AT_LEAST_ONCE";
        case FaultToleranceType::EXACTLY_ONCE: return "EXACTLY_ONCE";
        case FaultToleranceType::INVALID: return "INVALID";
    }
}
}// namespace NES
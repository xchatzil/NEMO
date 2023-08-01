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

#ifndef NES_CORE_INCLUDE_UTIL_REQUESTTYPE_HPP_
#define NES_CORE_INCLUDE_UTIL_REQUESTTYPE_HPP_

#include <string>

namespace NES {

class RequestType {
  public:
    /**
     * @brief Represents various request types.
     *
     * Add: Add query.
     * Stop: Stop query.
     * Restart: Restart query.
     * Fail: Fail query.
     * Migrate: Migrate query.
     * Update: Update running query.
     */
    enum Value : uint8_t { Add = 0, Stop, Restart, Fail, Migrate, Update };

    /**
     * @brief Get query status from string
     * @param queryStatus : string representation of query status
     * @return enum representing query status
     */
    static Value getFromString(const std::string queryStatus);

    /**
     * @brief Get query status in string representation
     * @param queryStatus : enum value of the query status
     * @return string representation of query status
     */
    static std::string toString(const Value queryStatus);
};

}// namespace NES

#endif// NES_CORE_INCLUDE_UTIL_REQUESTTYPE_HPP_

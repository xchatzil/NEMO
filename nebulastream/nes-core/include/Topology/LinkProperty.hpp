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

#ifndef NES_CORE_INCLUDE_TOPOLOGY_LINKPROPERTY_HPP_
#define NES_CORE_INCLUDE_TOPOLOGY_LINKPROPERTY_HPP_

#include <cstdint>

/**
 * @brief class to store link properties between two topology nodes
 */
class LinkProperty {
  public:
    LinkProperty(uint64_t bandwidth, uint64_t latency) : bandwidth(bandwidth), latency(latency) {}

    /**
     * @brief bandwidth in kilobytes
     */
    uint64_t bandwidth;

    /**
     * @brief latency in milliseconds
     */
    uint64_t latency;
};

using LinkPropertyPtr = std::shared_ptr<LinkProperty>;

#endif// NES_CORE_INCLUDE_TOPOLOGY_LINKPROPERTY_HPP_

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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_IDENTIFIERS_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_IDENTIFIERS_HPP_

#include <cstdint>

namespace NES {

using NodeId = uint64_t;
using SubpartitionId = uint64_t;
using PartitionId = uint64_t;
using OperatorId = uint64_t;
using OriginId = uint64_t;
using QueryId = uint64_t;
using SharedQueryId = uint64_t;
using QuerySubPlanId = uint64_t;
using TopologyNodeId = uint64_t;
static constexpr QueryId INVALID_QUERY_ID = 0;
static constexpr QuerySubPlanId INVALID_QUERY_SUB_PLAN_ID = 0;
static constexpr SharedQueryId INVALID_SHARED_QUERY_ID = 0;
static constexpr OperatorId INVALID_OPERATOR_ID = 0;
static constexpr OriginId INVALID_ORIGIN_ID = 0;
static constexpr TopologyNodeId INVALID_TOPOLOGY_NODE_ID = 0;

}// namespace NES

#endif// NES_DATA_TYPES_INCLUDE_COMMON_IDENTIFIERS_HPP_

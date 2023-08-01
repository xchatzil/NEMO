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
#ifndef NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPE_HPP_
#define NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPE_HPP_
#include <WorkerLocation.grpc.pb.h>
#include <string>

namespace NES::Spatial::Index::Experimental {

/**
 * this enum defines different types workers can have regarding their spatial information
 */
enum class NodeType {
    NO_LOCATION = 0,   //the worker does not have a known location
    FIXED_LOCATION = 1,//the worker has a known fixed location that will not change after its creation
    MOBILE_NODE = 2,   //the worker runs on a mobile device which might change its location anytime
    INVALID = 3        //no valid worker type
};
}// namespace NES::Spatial::Index::Experimental

#endif// NES_CORE_INCLUDE_UTIL_EXPERIMENTAL_NODETYPE_HPP_

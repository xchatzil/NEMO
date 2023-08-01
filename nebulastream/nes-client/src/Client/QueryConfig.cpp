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

#include <Client/QueryConfig.hpp>

namespace NES::Client {

QueryConfig::QueryConfig(FaultToleranceType::Value faultToleranceType,
                         LineageType::Value lineageType,
                         PlacementStrategy::Value placementType)
    : faultToleranceType(faultToleranceType), lineageType(lineageType), placementType(placementType) {}

FaultToleranceType::Value QueryConfig::getFaultToleranceType() const { return faultToleranceType; }

void QueryConfig::setFaultToleranceType(FaultToleranceType::Value faultToleranceType) {
    QueryConfig::faultToleranceType = faultToleranceType;
}

LineageType::Value QueryConfig::getLineageType() const { return lineageType; }

void QueryConfig::setLineageType(LineageType::Value lineageType) { QueryConfig::lineageType = lineageType; }

PlacementStrategy::Value QueryConfig::getPlacementType() const { return placementType; }

void QueryConfig::setPlacementType(PlacementStrategy::Value placementType) { QueryConfig::placementType = placementType; }

}// namespace NES::Client
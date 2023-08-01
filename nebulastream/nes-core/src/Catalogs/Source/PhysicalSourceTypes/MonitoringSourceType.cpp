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

#include <Catalogs/Source/PhysicalSourceTypes/MonitoringSourceType.hpp>
#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

namespace NES {

MonitoringSourceType::MonitoringSourceType(uint64_t metricCollectorType, std::chrono::milliseconds waitTime)
    : PhysicalSourceType(SourceType::MONITORING_SOURCE), metricCollectorType(metricCollectorType), waitTime(waitTime) {}

MonitoringSourceTypePtr MonitoringSourceType::create(uint64_t metricCollectorType, std::chrono::milliseconds waitTime) {
    return std::make_shared<MonitoringSourceType>(MonitoringSourceType(metricCollectorType, waitTime));
}

MonitoringSourceTypePtr MonitoringSourceType::create(uint64_t metricCollectorType) {
    return create(metricCollectorType, MonitoringSource::DEFAULT_WAIT_TIME);
}

std::string MonitoringSourceType::toString() {
    std::stringstream ss;
    ss << "MonitoringSource Type => {\n";
    ss << "waitTimeInMs:" + std::to_string(waitTime.count());
    ss << "metricCollectorType:" + NES::Monitoring::toString(Monitoring::MetricCollectorType(metricCollectorType));
    ss << "\n}";
    return ss.str();
}

bool MonitoringSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<MonitoringSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<MonitoringSourceType>();
    return waitTime == otherSourceConfig->waitTime && metricCollectorType == otherSourceConfig->metricCollectorType;
}

void MonitoringSourceType::reset() { setWaitTime(MonitoringSource::DEFAULT_WAIT_TIME); }
std::chrono::milliseconds MonitoringSourceType::getWaitTime() const { return waitTime; }
void MonitoringSourceType::setWaitTime(std::chrono::milliseconds waitTime) { this->waitTime = waitTime; }
uint64_t MonitoringSourceType::getMetricCollectorType() const { return metricCollectorType; }
void MonitoringSourceType::setMetricCollectorType(uint64_t metricCollectorType) {
    this->metricCollectorType = metricCollectorType;
}

}// namespace NES

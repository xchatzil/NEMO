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

#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Storage/LatestEntriesMetricStore.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/time.h>

namespace NES::Monitoring {

LatestEntriesMetricStore::LatestEntriesMetricStore() { NES_DEBUG("LatestEntriesMetricStore: Init NewestMetricStore"); }

MetricStoreType LatestEntriesMetricStore::getType() const { return NewestEntry; }

void LatestEntriesMetricStore::addMetrics(uint64_t nodeId, MetricPtr metric) {
    std::unique_lock lock(storeMutex);
    StoredNodeMetricsPtr nodeMetrics;
    auto metricType = metric->getMetricType();
    uint64_t timestamp = duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (storedMetrics.contains(nodeId)) {
        NES_TRACE("LatestEntriesMetricStore: Found stored metrics for node with ID " << nodeId);
        nodeMetrics = storedMetrics[nodeId];
        // check if the metric type exists
        if (nodeMetrics->contains(metricType)) {
            NES_TRACE("LatestEntriesMetricStore: Removing metrics " << nodeId << " of " << toString(metricType));
            nodeMetrics->at(metricType)->clear();
        } else {
            NES_TRACE("LatestEntriesMetricStore: Creating metrics " << nodeId << " of " << toString(metricType));
            nodeMetrics->insert({metricType, std::make_shared<std::vector<TimestampMetricPtr>>()});
        }
    } else {
        NES_TRACE("LatestEntriesMetricStore: Creating node " << nodeId << " of " << toString(metricType));
        nodeMetrics = std::make_shared<std::unordered_map<MetricType, std::shared_ptr<std::vector<TimestampMetricPtr>>>>();
        nodeMetrics->insert({metricType, std::make_shared<std::vector<TimestampMetricPtr>>()});
        storedMetrics.emplace(nodeId, nodeMetrics);
    }
    NES_TRACE("LatestEntriesMetricStore: Adding metrics for " << nodeId << " with type " << toString(metricType) << ": "
                                                              << NES::Monitoring::asJson(metric));
    TimestampMetricPtr entry = std::make_shared<std::pair<uint64_t, MetricPtr>>(timestamp, std::move(metric));
    auto entryVec = nodeMetrics->at(metricType);
    entryVec->emplace_back(std::move(entry));
}

bool LatestEntriesMetricStore::removeMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    if (storedMetrics.contains(nodeId)) {
        storedMetrics.erase(nodeId);
        return true;
    }
    return false;
}

bool LatestEntriesMetricStore::hasMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics.contains(nodeId);
}

StoredNodeMetricsPtr LatestEntriesMetricStore::getAllMetrics(uint64_t nodeId) {
    std::unique_lock lock(storeMutex);
    return storedMetrics[nodeId];
}

}// namespace NES::Monitoring
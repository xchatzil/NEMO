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

#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/MonitoringSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/Wrapper/CpuMetricsWrapper.hpp>
#include <Monitoring/Storage/AbstractMetricStore.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

namespace NES {
MonitoringSink::MonitoringSink(SinkFormatPtr sinkFormat,
                               Monitoring::MetricStorePtr metricStore,
                               Monitoring::MetricCollectorType collectorType,
                               Runtime::NodeEnginePtr nodeEngine,
                               uint32_t numOfProducers,
                               QueryId queryId,
                               QuerySubPlanId querySubPlanId,
                               FaultToleranceType::Value faultToleranceType,
                               uint64_t numberOfOrigins)
    : SinkMedium(std::move(sinkFormat),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      metricStore(metricStore), collectorType(collectorType) {
    NES_ASSERT(metricStore != nullptr, "MonitoringSink: MetricStore is null.");
}

MonitoringSink::~MonitoringSink() = default;

SinkMediumTypes MonitoringSink::getSinkMediumType() { return MONITORING_SINK; }

bool MonitoringSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);

    if (!inputBuffer) {
        throw Exceptions::RuntimeException("MonitoringSink::writeData input buffer invalid");
    }

    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto& buffer : dataBuffers) {
        Monitoring::MetricPtr parsedMetric = Monitoring::MetricUtils::createMetricFromCollectorType(collectorType);
        Monitoring::readFromBuffer(parsedMetric, buffer, 0);
        auto nodeIdPtr = (uint64_t*) buffer.getBuffer();
        uint64_t nodeId = nodeIdPtr[0];
        NES_TRACE("MonitoringSink: Received buffer for " << nodeId << " with " << inputBuffer.getNumberOfTuples()
                                                         << " tuple and size " << getSchemaPtr()->getSchemaSizeInBytes() << ": "
                                                         << asJson(parsedMetric));

        metricStore->addMetrics(nodeId, std::move(parsedMetric));
    }

    updateWatermarkCallback(inputBuffer);

    return true;
}

std::string MonitoringSink::toString() const {
    std::stringstream ss;
    ss << "MONITORING_SINK(";
    ss << "COLLECTOR(" << NES::Monitoring::toString(collectorType) << ")";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void MonitoringSink::setup() {
    // currently not required
}

void MonitoringSink::shutdown() {
    // currently not required
}

}// namespace NES
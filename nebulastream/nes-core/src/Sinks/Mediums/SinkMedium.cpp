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

#include <API/Schema.hpp>
#include <Components/NesWorker.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat,
                       Runtime::NodeEnginePtr nodeEngine,
                       uint32_t numOfProducers,
                       QueryId queryId,
                       QuerySubPlanId querySubPlanId,
                       FaultToleranceType::Value faultToleranceType,
                       uint64_t numberOfOrigins,
                       Windowing::MultiOriginWatermarkProcessorPtr watermarkProcessor)
    : sinkFormat(std::move(sinkFormat)), nodeEngine(std::move(nodeEngine)), activeProducers(numOfProducers), queryId(queryId),
      querySubPlanId(querySubPlanId), faultToleranceType(faultToleranceType), numberOfOrigins(numberOfOrigins),
      watermarkProcessor(std::move(watermarkProcessor)) {
    bufferCount = 0;
    buffersPerEpoch = this->nodeEngine->getQueryManager()->getNumberOfBuffersPerEpoch();
    NES_ASSERT2_FMT(numOfProducers > 0, "Invalid num of producers on Sink");
    NES_ASSERT2_FMT(this->nodeEngine, "Invalid node engine");
    if (faultToleranceType == FaultToleranceType::AT_LEAST_ONCE) {
        updateWatermarkCallback = [this](Runtime::TupleBuffer& inputBuffer) {
            updateWatermark(inputBuffer);
        };
    } else {
        updateWatermarkCallback = [](Runtime::TupleBuffer&) {
        };
    }
}

uint64_t SinkMedium::getNumberOfWrittenOutBuffers() {
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}

void SinkMedium::updateWatermark(Runtime::TupleBuffer& inputBuffer) {
    std::unique_lock lock(writeMutex);
    NES_ASSERT(watermarkProcessor != nullptr, "SinkMedium::updateWatermark watermark processor is null");
    watermarkProcessor->updateWatermark(inputBuffer.getWatermark(), inputBuffer.getSequenceNumber(), inputBuffer.getOriginId());
    if (!(bufferCount % buffersPerEpoch) && bufferCount != 0) {
        auto timestamp = watermarkProcessor->getCurrentWatermark();
        if (timestamp) {
            notifyEpochTermination(timestamp);
        }
    }
    bufferCount++;
}

uint64_t SinkMedium::getNumberOfWrittenOutTuples() {
    std::unique_lock lock(writeMutex);
    return sentTuples;
}

SchemaPtr SinkMedium::getSchemaPtr() const { return sinkFormat->getSchemaPtr(); }

std::string SinkMedium::getSinkFormat() { return sinkFormat->toString(); }

bool SinkMedium::getAppendAsBool() const { return append; }

QuerySubPlanId SinkMedium::getParentPlanId() const { return querySubPlanId; }

QueryId SinkMedium::getQueryId() const { return queryId; }

std::string SinkMedium::getAppendAsString() const {
    if (append) {
        return "APPEND";
    }
    return "OVERWRITE";
}

bool SinkMedium::notifyEpochTermination(uint64_t epochBarrier) const {
    uint64_t queryId = nodeEngine->getQueryManager()->getQueryId(querySubPlanId);
    NES_ASSERT(queryId >= 0, "SinkMedium: no queryId found for querySubPlanId");
    if (auto listener = nodeEngine->getQueryStatusListener(); listener) {
        bool success = listener->notifyEpochTermination(epochBarrier, queryId);
        if (success) {
            return true;
        }
    }
    return false;
}

void SinkMedium::reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) {
    Reconfigurable::reconfigure(message, context);
}

uint64_t SinkMedium::getCurrentEpochBarrier() { return watermarkProcessor->getCurrentWatermark(); }

void SinkMedium::postReconfigurationCallback(Runtime::ReconfigurationMessage& message) {
    Reconfigurable::postReconfigurationCallback(message);
    Runtime::QueryTerminationType terminationType = Runtime::QueryTerminationType::Invalid;
    switch (message.getType()) {
        case Runtime::FailEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Failure;
            break;
        }
        case Runtime::SoftEndOfStream: {
            terminationType = Runtime::QueryTerminationType::Graceful;
            break;
        }
        case Runtime::HardEndOfStream: {
            terminationType = Runtime::QueryTerminationType::HardStop;
            break;
        }
        default: {
            break;
        }
    }
    if (terminationType != Runtime::QueryTerminationType::Invalid) {
        NES_DEBUG("Got EoS on Sink " << toString());
        if (activeProducers.fetch_sub(1) == 1) {
            shutdown();
            nodeEngine->getQueryManager()->notifySinkCompletion(querySubPlanId,
                                                                std::static_pointer_cast<SinkMedium>(shared_from_this()),
                                                                terminationType);
            NES_DEBUG("Sink [" << toString() << "] is completed with " << terminationType);
        }
    }
}
}// namespace NES

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

#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/ZmqSink.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstdint>

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <zmq.hpp>

namespace NES {

SinkMediumTypes ZmqSink::getSinkMediumType() { return ZMQ_SINK; }

ZmqSink::ZmqSink(SinkFormatPtr format,
                 Runtime::NodeEnginePtr nodeEngine,
                 uint32_t numOfProducers,
                 const std::string& host,
                 uint16_t port,
                 bool internal,
                 QueryId queryId,
                 QuerySubPlanId querySubPlanId,
                 FaultToleranceType::Value faultToleranceType,
                 uint64_t numberOfOrigins)
    : SinkMedium(std::move(format),
                 std::move(nodeEngine),
                 numOfProducers,
                 queryId,
                 querySubPlanId,
                 faultToleranceType,
                 numberOfOrigins,
                 std::make_unique<Windowing::MultiOriginWatermarkProcessor>(numberOfOrigins)),
      host(host.substr(0, host.find(':'))), port(port), internal(internal), context(zmq::context_t(1)),
      socket(zmq::socket_t(context, ZMQ_PUSH)) {
    NES_DEBUG("ZmqSink  " << this << ": Init ZMQ Sink to " << host << ":" << port);
}

ZmqSink::~ZmqSink() {
    NES_DEBUG("ZmqSink::~ZmqSink: destructor called");
    bool success = disconnect();
    if (success) {
        NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
    } else {
        /// XXX:
        NES_ERROR("ZmqSink  " << this << ": Destroy ZMQ Sink failed cause it could not be disconnected");
        NES_ASSERT2_FMT(false, "ZMQ Sink destruction failed");
    }
    NES_DEBUG("ZmqSink  " << this << ": Destroy ZMQ Sink");
}

bool ZmqSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    std::unique_lock lock(writeMutex);// TODO this is an anti-pattern in ZMQ
    connect();
    if (!connected) {
        NES_DEBUG("ZmqSink  " << this << ": cannot write buffer " << inputBuffer << " because queue is not connected");
        throw Exceptions::RuntimeException("Write to zmq sink failed");
    }

    if (!inputBuffer) {
        NES_ERROR("ZmqSink::writeData input buffer invalid");
        return false;
    }

    if (!schemaWritten && !internal) {//TODO:atomic
        NES_DEBUG("FileSink::getData: write schema");
        auto schemaBuffer = sinkFormat->getSchema();
        if (schemaBuffer) {
            NES_DEBUG("ZmqSink writes schema buffer");
            try {

                // Send Header
                std::array<uint64_t, 2> const envelopeData{schemaBuffer->getNumberOfTuples(), schemaBuffer->getWatermark()};
                constexpr auto envelopeSize = sizeof(uint64_t) * 2;
                static_assert(envelopeSize == sizeof(envelopeData));
                zmq::message_t envelope{&(envelopeData[0]), envelopeSize};
                if (auto const sentEnvelopeSize = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                    sentEnvelopeSize != envelopeSize) {
                    NES_DEBUG("ZmqSink  " << this << ": schema send NOT successful");
                    return false;
                }

                // Send payload
                zmq::mutable_buffer payload{schemaBuffer->getBuffer(), schemaBuffer->getBufferSize()};
                if (auto const sentPayloadSize = socket.send(payload, zmq::send_flags::none).value_or(0);
                    sentPayloadSize != payload.size()) {
                    NES_DEBUG("ZmqSink  " << this << ": sending payload failed.");
                    return false;
                }
                NES_DEBUG("ZmqSink  " << this << ": schema send successful");

            } catch (const zmq::error_t& ex) {
                if (ex.num() != ETERM) {
                    NES_ERROR("ZmqSink: schema write " << ex.what());
                }
            }
            schemaWritten = true;
            NES_DEBUG("ZmqSink::writeData: schema written");
        }
        { NES_DEBUG("ZmqSink::writeData: no schema written"); }
    } else {
        NES_DEBUG("ZmqSink::getData: schema already written");
    }

    NES_DEBUG("ZmqSink  " << this << ": writes buffer " << inputBuffer << " with tupleCnt =" << inputBuffer.getNumberOfTuples()
                          << " watermark=" << inputBuffer.getWatermark());
    auto dataBuffers = sinkFormat->getData(inputBuffer);
    for (auto buffer : dataBuffers) {// XXX: Is it actually our intention to iterate over buffers until no exception is thrown?
        try {
            ++sentBuffer;

            // Create envelope
            std::array<uint64_t, 2> const envelopeData{buffer.getNumberOfTuples(), buffer.getWatermark()};
            static_assert(sizeof(envelopeData) == sizeof(uint64_t) * 2);
            zmq::message_t envelope{&(envelopeData[0]), sizeof(envelopeData)};
            if (auto const sentEnvelope = socket.send(envelope, zmq::send_flags::sndmore).value_or(0);
                sentEnvelope != sizeof(envelopeData)) {
                NES_WARNING("ZmqSink  " << this << ": data payload send NOT successful");
                return false;
            }

            // Create message.
            // Copying the entire payload here to avoid UB.
            zmq::message_t payload{buffer.getBuffer(), buffer.getBufferSize()};
            if (auto const sentPayload = socket.send(payload, zmq::send_flags::none).value_or(0);
                sentPayload != buffer.getBufferSize()) {
                NES_WARNING("ZmqSink  " << this << ": data send NOT successful");
                return false;
            }
            NES_DEBUG("ZmqSink  " << this << ": data send successful");
            return true;

        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZmqSink: " << ex.what());
            }
        }
    }
    updateWatermarkCallback(inputBuffer);
    return true;
}

std::string ZmqSink::toString() const {
    std::stringstream ss;
    ss << "ZMQ_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
    ss << "HOST=" << host << ", ";
    ss << "PORT=" << port;
    ss << ")";
    return ss.str();
}

bool ZmqSink::connect() {
    if (!connected) {
        try {
            NES_DEBUG("ZmqSink: connect to address=" << host << " port=" << port);
            auto address = std::string("tcp://") + host + std::string(":") + std::to_string(port);
            socket.connect(address.c_str());
            connected = true;
        } catch (const zmq::error_t& ex) {
            // recv() throws ETERM when the zmq context is destroyed,
            //  as when AsyncZmqListener::Stop() is called
            if (ex.num() != ETERM) {
                NES_ERROR("ZmqSink: " << ex.what());
            }
        }
    }
    if (connected) {
        NES_DEBUG("ZmqSink  " << this << ": connected address=" << host << " port= " << port);
    } else {
        NES_DEBUG("ZmqSink  " << this << ": NOT connected=" << host << " port= " << port);
    }
    return connected;
}

bool ZmqSink::disconnect() {
    if (connected) {
        socket.close();
        connected = false;
    }
    if (!connected) {
        NES_DEBUG("ZmqSink  " << this << ": disconnected");
    } else {
        NES_DEBUG("ZmqSink  " << this << ": NOT disconnected");
    }
    return !connected;
}

int ZmqSink::getPort() const { return this->port; }

std::string ZmqSink::getHost() const { return host; }

}// namespace NES

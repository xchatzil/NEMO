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

#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>

#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

MemoryMetrics::MemoryMetrics()
    : nodeId(0), timestamp(0), TOTAL_RAM(0), TOTAL_SWAP(0), FREE_RAM(0), SHARED_RAM(0), BUFFER_RAM(0), FREE_SWAP(0),
      TOTAL_HIGH(0), FREE_HIGH(0), PROCS(0), MEM_UNIT(0), LOADS_1MIN(0), LOADS_5MIN(0), LOADS_15MIN(0) {}

SchemaPtr MemoryMetrics::getSchema(const std::string& prefix) {
    auto schema = Schema::create(Schema::ROW_LAYOUT)
                      ->addField(prefix + "node_id", BasicType::UINT64)
                      ->addField(prefix + "timestamp", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_RAM", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_SWAP", BasicType::UINT64)
                      ->addField(prefix + "FREE_RAM", BasicType::UINT64)
                      ->addField(prefix + "SHARED_RAM", BasicType::UINT64)
                      ->addField(prefix + "BUFFER_RAM", BasicType::UINT64)
                      ->addField(prefix + "FREE_SWAP", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_HIGH", BasicType::UINT64)
                      ->addField(prefix + "FREE_HIGH", BasicType::UINT64)
                      ->addField(prefix + "PROCS", BasicType::UINT64)
                      ->addField(prefix + "MEM_UNIT", BasicType::UINT64)
                      ->addField(prefix + "LOADS_1MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_5MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_15MIN", BasicType::UINT64);
    return schema;
}

void MemoryMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(MemoryMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = MemoryMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "MemoryMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestamp);
    buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_RAM);
    buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_SWAP);
    buffer[tupleIndex][cnt++].write<uint64_t>(FREE_RAM);
    buffer[tupleIndex][cnt++].write<uint64_t>(SHARED_RAM);
    buffer[tupleIndex][cnt++].write<uint64_t>(BUFFER_RAM);
    buffer[tupleIndex][cnt++].write<uint64_t>(FREE_SWAP);
    buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_HIGH);
    buffer[tupleIndex][cnt++].write<uint64_t>(FREE_HIGH);
    buffer[tupleIndex][cnt++].write<uint64_t>(PROCS);
    buffer[tupleIndex][cnt++].write<uint64_t>(MEM_UNIT);
    buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_1MIN);
    buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_5MIN);
    buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_15MIN);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void MemoryMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(MemoryMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestamp = buffer[tupleIndex][cnt++].read<uint64_t>();
    TOTAL_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    TOTAL_SWAP = buffer[tupleIndex][cnt++].read<uint64_t>();
    FREE_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    SHARED_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    BUFFER_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    FREE_SWAP = buffer[tupleIndex][cnt++].read<uint64_t>();
    TOTAL_HIGH = buffer[tupleIndex][cnt++].read<uint64_t>();
    FREE_HIGH = buffer[tupleIndex][cnt++].read<uint64_t>();
    PROCS = buffer[tupleIndex][cnt++].read<uint64_t>();
    MEM_UNIT = buffer[tupleIndex][cnt++].read<uint64_t>();
    LOADS_1MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
    LOADS_5MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
    LOADS_15MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
}

SchemaPtr getSchema(const MemoryMetrics&, const std::string& prefix) { return MemoryMetrics::getSchema(prefix); }

bool MemoryMetrics::operator==(const MemoryMetrics& rhs) const {
    return nodeId == rhs.nodeId && timestamp == rhs.timestamp && TOTAL_RAM == rhs.TOTAL_RAM && TOTAL_SWAP == rhs.TOTAL_SWAP
        && FREE_RAM == rhs.FREE_RAM && SHARED_RAM == rhs.SHARED_RAM && BUFFER_RAM == rhs.BUFFER_RAM && FREE_SWAP == rhs.FREE_SWAP
        && TOTAL_HIGH == rhs.TOTAL_HIGH && FREE_HIGH == rhs.FREE_HIGH && PROCS == rhs.PROCS && MEM_UNIT == rhs.MEM_UNIT
        && LOADS_1MIN == rhs.LOADS_1MIN && LOADS_5MIN == rhs.LOADS_5MIN && LOADS_15MIN == rhs.LOADS_15MIN;
}

bool MemoryMetrics::operator!=(const MemoryMetrics& rhs) const { return !(rhs == *this); }

nlohmann::json MemoryMetrics::toJson() const {
    nlohmann::json metricsJson{};
    metricsJson["NODE_ID"] = nodeId;
    metricsJson["TIMESTAMP"] = timestamp;
    metricsJson["TOTAL_RAM"] = TOTAL_RAM;
    metricsJson["TOTAL_SWAP"] = TOTAL_SWAP;
    metricsJson["FREE_RAM"] = FREE_RAM;
    metricsJson["SHARED_RAM"] = SHARED_RAM;
    metricsJson["BUFFER_RAM"] = BUFFER_RAM;
    metricsJson["FREE_SWAP"] = FREE_SWAP;
    metricsJson["TOTAL_HIGH"] = TOTAL_HIGH;
    metricsJson["FREE_HIGH"] = FREE_HIGH;
    metricsJson["PROCS"] = PROCS;
    metricsJson["MEM_UNIT"] = MEM_UNIT;
    metricsJson["LOADS_1MIN"] = LOADS_1MIN;
    metricsJson["LOADS_5MIN"] = LOADS_5MIN;
    metricsJson["LOADS_15MIN"] = LOADS_15MIN;
    return metricsJson;
}

void writeToBuffer(const MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const MemoryMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
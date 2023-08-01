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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Monitoring/Metrics/Gauge/RuntimeMetrics.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

RuntimeMetrics::RuntimeMetrics()
    : nodeId(0), wallTimeNs(0), memoryUsageInBytes(0), cpuLoadInJiffies(0), blkioBytesRead(0), blkioBytesWritten(0),
      batteryStatusInPercent(0), latCoord(0), longCoord(0) {
    NES_DEBUG("RuntimeMetrics: Default ctor");
}

SchemaPtr RuntimeMetrics::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "wallTimeNs", BasicType::UINT64)
                           ->addField(prefix + "memoryUsageInBytes", BasicType::UINT64)
                           ->addField(prefix + "cpuLoadInJiffies", BasicType::UINT64)
                           ->addField(prefix + "blkioBytesRead", BasicType::UINT64)
                           ->addField(prefix + "blkioBytesWritten", BasicType::UINT64)
                           ->addField(prefix + "batteryStatusInPercent", BasicType::UINT64)
                           ->addField(prefix + "latCoord", BasicType::UINT64)
                           ->addField(prefix + "longCoord", BasicType::UINT64);
    return schema;
}

void RuntimeMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(RuntimeMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = RuntimeMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "RuntimeMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(wallTimeNs);
    buffer[tupleIndex][cnt++].write<uint64_t>(memoryUsageInBytes);
    buffer[tupleIndex][cnt++].write<uint64_t>(cpuLoadInJiffies);
    buffer[tupleIndex][cnt++].write<uint64_t>(blkioBytesRead);
    buffer[tupleIndex][cnt++].write<uint64_t>(blkioBytesWritten);
    buffer[tupleIndex][cnt++].write<uint64_t>(batteryStatusInPercent);
    buffer[tupleIndex][cnt++].write<uint64_t>(latCoord);
    buffer[tupleIndex][cnt++].write<uint64_t>(longCoord);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void RuntimeMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(RuntimeMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);
    uint64_t cnt = 0;

    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    wallTimeNs = buffer[tupleIndex][cnt++].read<uint64_t>();
    memoryUsageInBytes = buffer[tupleIndex][cnt++].read<uint64_t>();
    cpuLoadInJiffies = buffer[tupleIndex][cnt++].read<uint64_t>();
    blkioBytesRead = buffer[tupleIndex][cnt++].read<uint64_t>();
    blkioBytesWritten = buffer[tupleIndex][cnt++].read<uint64_t>();
    batteryStatusInPercent = buffer[tupleIndex][cnt++].read<uint64_t>();
    latCoord = buffer[tupleIndex][cnt++].read<uint64_t>();
    longCoord = buffer[tupleIndex][cnt++].read<uint64_t>();
}

nlohmann::json RuntimeMetrics::toJson() const {
    nlohmann::json metricsJson{};

    metricsJson["NODE_ID"] = nodeId;
    metricsJson["WallClockNs"] = wallTimeNs;
    metricsJson["MemoryUsageInBytes"] = memoryUsageInBytes;
    metricsJson["CpuLoadInJiffies"] = cpuLoadInJiffies;
    metricsJson["BlkioBytesRead"] = blkioBytesRead;
    metricsJson["BlkioBytesWritten"] = blkioBytesWritten;
    metricsJson["BatteryStatus"] = batteryStatusInPercent;
    metricsJson["LatCoord"] = latCoord;
    metricsJson["LongCoord"] = longCoord;

    return metricsJson;
}

bool RuntimeMetrics::operator==(const RuntimeMetrics& rhs) const {
    return nodeId == rhs.nodeId && wallTimeNs == rhs.wallTimeNs && memoryUsageInBytes == rhs.memoryUsageInBytes
        && cpuLoadInJiffies == rhs.cpuLoadInJiffies && blkioBytesRead == rhs.blkioBytesRead
        && blkioBytesWritten == rhs.blkioBytesWritten && batteryStatusInPercent == rhs.batteryStatusInPercent
        && latCoord == rhs.latCoord && longCoord == rhs.longCoord;
}

bool RuntimeMetrics::operator!=(const RuntimeMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(RuntimeMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const RuntimeMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring

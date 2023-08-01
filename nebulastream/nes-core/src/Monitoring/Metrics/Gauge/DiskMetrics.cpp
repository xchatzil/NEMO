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
#include <Monitoring/Metrics/Gauge/DiskMetrics.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

DiskMetrics::DiskMetrics() : nodeId(0), timestamp(0), fBsize(0), fFrsize(0), fBlocks(0), fBfree(0), fBavail(0) {}

SchemaPtr DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64)
                           ->addField(prefix + "timestamp", BasicType::UINT64)
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void DiskMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(DiskMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = DiskMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "DiskMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestamp);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBsize);
    buffer[tupleIndex][cnt++].write<uint64_t>(fFrsize);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBlocks);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBfree);
    buffer[tupleIndex][cnt++].write<uint64_t>(fBavail);

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void DiskMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(DiskMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    int cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestamp = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    fFrsize = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBlocks = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBfree = buffer[tupleIndex][cnt++].read<uint64_t>();
    fBavail = buffer[tupleIndex][cnt++].read<uint64_t>();
}

nlohmann::json DiskMetrics::toJson() const {
    nlohmann::json metricsJson{};
    metricsJson["NODE_ID"] = nodeId;
    metricsJson["TIMESTAMP"] = timestamp;
    metricsJson["F_BSIZE"] = fBsize;
    metricsJson["F_FRSIZE"] = fFrsize;
    metricsJson["F_BLOCKS"] = fBlocks;
    metricsJson["F_BFREE"] = fBfree;
    metricsJson["F_BAVAIL"] = fBavail;
    return metricsJson;
}

bool DiskMetrics::operator==(const DiskMetrics& rhs) const {
    return nodeId == rhs.nodeId && timestamp == rhs.timestamp && fBavail == rhs.fBavail && fBfree == rhs.fBfree
        && fBlocks == rhs.fBlocks && fBsize == rhs.fBsize && fFrsize == rhs.fFrsize;
}

bool DiskMetrics::operator!=(const DiskMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const DiskMetrics& metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metric.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(DiskMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const DiskMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring
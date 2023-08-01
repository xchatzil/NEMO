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
#include <Common/DataTypes/BasicTypes.hpp>
#include <DataGeneration/ZipfianDataGenerator.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Util/ZipfianGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

ZipfianDataGenerator::ZipfianDataGenerator(double alpha, uint64_t minValue, uint64_t maxValue)
    : DataGenerator(), alpha(alpha), minValue(minValue), maxValue(maxValue) {}

NES::SchemaPtr ZipfianDataGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("id", NES::UINT64))
        ->addField(createField("value", NES::UINT64))
        ->addField(createField("payload", NES::UINT64))
        ->addField(createField("timestamp", NES::UINT64));
}

std::string ZipfianDataGenerator::getName() { return "Zipfian"; }

std::vector<Runtime::TupleBuffer> ZipfianDataGenerator::createData(size_t numberOfBuffers, size_t bufferSize) {
    std::vector<Runtime::TupleBuffer> createdBuffers;
    createdBuffers.reserve(numberOfBuffers);

    auto memoryLayout = this->getMemoryLayout(bufferSize);
    NES_INFO("Zipfian source mode");

    // Prints every five percent the current progress
    uint64_t noTuplesInFivePercent = std::max(1UL, (numberOfBuffers * 5) / 100);
    for (uint64_t curBuffer = 0; curBuffer < numberOfBuffers; ++curBuffer) {

        Runtime::TupleBuffer bufferRef = allocateBuffer();
        auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayout, bufferRef);

        // using seed to generate a predictable sequence of values for deterministic behavior
        std::mt19937 generator(GENERATOR_SEED_ZIPFIAN);
        ZipfianGenerator zipfianGenerator(minValue, maxValue, alpha);

        /* This branch is solely for performance reasons.
             It still works with all layouts, for a RowLayout it is just magnitudes faster with this branch */
        if (memoryLayout->getSchema()->getLayoutType() == Schema::ROW_LAYOUT) {
            auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(memoryLayout->getSchema(), bufferSize);
            auto rowLayoutBuffer = rowLayout->bind(bufferRef);

            /*
                 * Iterating over all tuples of the current buffer and insert the tuples according to the schema.
                 * The value is drawn from the zipfianGenerator and thus has a zipfian shape.
                 * As we do know the memory layout, we can use the custom pushRecord() method to
                 */
            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                uint64_t value = zipfianGenerator(generator);
                rowLayoutBuffer->pushRecord<false>(
                    std::tuple<uint64_t, uint64_t, uint64_t, uint64_t>(curRecord, value, curRecord, curRecord));
            }

        } else {

            /*
                 * Iterating over all tuples of the current buffer and insert the tuples according to the schema.
                 * The value is drawn from the zipfianGenerator and thus has a zipfian shape.
                 * As we do not know the memory layout, we use dynamic field handlers
                 */
            for (uint64_t curRecord = 0; curRecord < dynamicBuffer.getCapacity(); ++curRecord) {
                auto value = zipfianGenerator(generator);
                dynamicBuffer[curRecord]["id"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["value"].write<uint64_t>(value);
                dynamicBuffer[curRecord]["payload"].write<uint64_t>(curRecord);
                dynamicBuffer[curRecord]["timestamp"].write<uint64_t>(curRecord);
            }
        }

        if (curBuffer % noTuplesInFivePercent == 0) {
            NES_INFO("ZipfianDataGenerator: currently at " << (((double) curBuffer / numberOfBuffers) * 100) << "%");
        }

        dynamicBuffer.setNumberOfTuples(dynamicBuffer.getCapacity());
        createdBuffers.emplace_back(bufferRef);
    }

    NES_INFO("Created all buffers!");
    return createdBuffers;
}
std::string ZipfianDataGenerator::toString() {
    std::ostringstream oss;

    oss << getName() << " (" << minValue << ", " << maxValue << ", " << alpha << ")";

    return oss.str();
}

}// namespace NES::Benchmark::DataGeneration
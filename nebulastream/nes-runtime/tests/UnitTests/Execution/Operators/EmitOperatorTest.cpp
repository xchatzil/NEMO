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
#include <Execution/MemoryProvider/ColumnMemoryProvider.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Emit.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class EmitOperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EmitOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup EmitOperatorTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down EmitOperatorTest test class."); }
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext()
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            {}){
            // nop
        };

    std::vector<TupleBuffer> buffers;
};

/**
 * @brief Emit operator that emits a row oriented tuple buffer.
 */
TEST_F(EmitOperatorTest, emitRecordsToRowBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);

    auto pipelineContext = MockedPipelineExecutionContext();
    auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());
    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    auto emitOperator = Emit(std::move(memoryProviderPtr));
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    emitOperator.open(ctx, recordBuffer);
    for (uint64_t i = 0; i < rowMemoryLayout->getCapacity(); i++) {
        auto record = Record({{"f1", Value<>(i)}, {"f2", Value<>(10)}});
        emitOperator.execute(ctx, record);
    }
    emitOperator.close(ctx, recordBuffer);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto buffer = pipelineContext.buffers[0];
    EXPECT_EQ(buffer.getNumberOfTuples(), rowMemoryLayout->getCapacity());

    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowMemoryLayout, buffer);
    for (uint64_t i = 0; i < rowMemoryLayout->getCapacity(); i++) {
        EXPECT_EQ(dynamicBuffer[i]["f1"].read<int64_t>(), i);
    }
}

/**
 * @brief Emit operator that outputs multiple tuple buffer in row layout.
 */
TEST_F(EmitOperatorTest, emitRecordsToRowBufferWithOverflow) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto rowMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(schema, bm->getBufferSize());

    auto pipelineContext = MockedPipelineExecutionContext();
    auto memoryProviderPtr = std::make_unique<MemoryProvider::RowMemoryProvider>(rowMemoryLayout);
    auto emitOperator = Emit(std::move(memoryProviderPtr));
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    emitOperator.open(ctx, recordBuffer);
    for (uint64_t i = 0; i < rowMemoryLayout->getCapacity() * 2; i++) {
        auto record = Record({{"f1", Value<>(i)}, {"f2", Value<>(10)}});
        emitOperator.execute(ctx, record);
    }
    emitOperator.close(ctx, recordBuffer);

    EXPECT_EQ(pipelineContext.buffers.size(), 2);
    auto buffer = pipelineContext.buffers[0];
    EXPECT_EQ(buffer.getNumberOfTuples(), rowMemoryLayout->getCapacity());
}

/**
 * @brief Emit operator that emits a column oriented tuple buffer.
 */
TEST_F(EmitOperatorTest, emitRecordsToColumnBuffer) {
    auto bm = std::make_shared<Runtime::BufferManager>();
    auto wc = std::make_shared<WorkerContext>(0, bm, 100);
    auto schema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT);
    schema->addField("f1", BasicType::INT64);
    schema->addField("f2", BasicType::INT64);
    auto columnMemoryLayout = Runtime::MemoryLayouts::ColumnLayout::create(schema, bm->getBufferSize());

    auto pipelineContext = MockedPipelineExecutionContext();
    auto memoryProviderPtr = std::make_unique<MemoryProvider::ColumnMemoryProvider>(columnMemoryLayout);
    auto emitOperator = Emit(std::move(memoryProviderPtr));
    auto ctx = ExecutionContext(Value<MemRef>((int8_t*) wc.get()), Value<MemRef>((int8_t*) &pipelineContext));
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    emitOperator.open(ctx, recordBuffer);
    for (uint64_t i = 0; i < columnMemoryLayout->getCapacity(); i++) {
        auto record = Record({{"f1", Value<>(i)}, {"f2", Value<>(10)}});
        emitOperator.execute(ctx, record);
    }
    emitOperator.close(ctx, recordBuffer);

    EXPECT_EQ(pipelineContext.buffers.size(), 1);
    auto buffer = pipelineContext.buffers[0];
    EXPECT_EQ(buffer.getNumberOfTuples(), columnMemoryLayout->getCapacity());

    auto dynamicBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(columnMemoryLayout, buffer);
    for (uint64_t i = 0; i < columnMemoryLayout->getCapacity(); i++) {
        EXPECT_EQ(dynamicBuffer[i]["f1"].read<int64_t>(), i);
    }
}

}// namespace NES::Runtime::Execution::Operators
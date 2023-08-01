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

// clang-format: off
#include "gtest/gtest.h"
// clang-format: on
#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <NesBaseTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/GPURuntime/CUDAKernelWrapper.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/SourceCreator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/DummySink.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SchemaSourceDescriptor.hpp>
#include <Util/TestQuery.hpp>
#include <Util/TestQueryCompiler.hpp>
#include <Util/TestSink.hpp>
#include <Util/TestUtils.hpp>
#include <cuda.h>
#include <cuda_runtime.h>
#include <iostream>
#include <utility>

using namespace NES;
using Runtime::TupleBuffer;

#define NUMBER_OF_TUPLE 10

class GPUQueryExecutionTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("GPUQueryExecutionTest.log", NES::LogLevel::LOG_DEBUG); }
    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        testSchemaSimple = Schema::create()->addField("test$value", BasicType::INT32);
        testSchemaMultipleFields = Schema::create()
                                       ->addField("test$id", BasicType::INT64)
                                       ->addField("test$one", BasicType::INT64)
                                       ->addField("test$value", BasicType::INT64);
        testSchemaColumnLayout = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                     ->addField("test$id", BasicType::INT64)
                                     ->addField("test$one", BasicType::INT64)
                                     ->addField("test$value", BasicType::INT64);
        auto defaultSourceType = DefaultSourceType::create();
        PhysicalSourcePtr sourceConf = PhysicalSource::create("default", "default1", defaultSourceType);
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->physicalSources.add(sourceConf);

        nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                         .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                         .build();
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { ASSERT_TRUE(nodeEngine->stop()); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {}

    SchemaPtr testSchemaSimple;
    SchemaPtr testSchemaMultipleFields;
    SchemaPtr testSchemaColumnLayout;
    Runtime::NodeEnginePtr nodeEngine;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
};

void cleanUpPlan(Runtime::Execution::ExecutableQueryPlanPtr plan) {
    std::for_each(plan->getSources().begin(), plan->getSources().end(), [plan](auto source) {
        plan->notifySourceCompletion(source, Runtime::QueryTerminationType::Graceful);
    });
    std::for_each(plan->getPipelines().begin(), plan->getPipelines().end(), [plan](auto pipeline) {
        plan->notifyPipelineCompletion(pipeline, Runtime::QueryTerminationType::Graceful);
    });
    std::for_each(plan->getSinks().begin(), plan->getSinks().end(), [plan](auto sink) {
        plan->notifySinkCompletion(sink, Runtime::QueryTerminationType::Graceful);
    });
    ASSERT_TRUE(plan->stop());
}

void fillBufferToSimpleSchema(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout) {

    auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < NUMBER_OF_TUPLE; recordIndex++) {
        valueField[recordIndex] = recordIndex;
    }
    buf.setNumberOfTuples(NUMBER_OF_TUPLE);
}

void fillBufferToMultiFieldSchema(TupleBuffer& buf, const Runtime::MemoryLayouts::RowLayoutPtr& memoryLayout) {

    auto recordIndexFields = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < 10; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 1;
        fields02[recordIndex] = recordIndex % 2;
    }
    buf.setNumberOfTuples(NUMBER_OF_TUPLE);
}

void fillBufferColumnLayout(TupleBuffer& buf, const Runtime::MemoryLayouts::ColumnLayoutPtr& memoryLayout) {

    auto recordIndexFields = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(0, memoryLayout, buf);
    auto fields01 = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(1, memoryLayout, buf);
    auto fields02 = Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(2, memoryLayout, buf);

    for (int recordIndex = 0; recordIndex < NUMBER_OF_TUPLE; recordIndex++) {
        recordIndexFields[recordIndex] = recordIndex;
        fields01[recordIndex] = 1;
        fields02[recordIndex] = recordIndex % 2;
    }
    buf.setNumberOfTuples(NUMBER_OF_TUPLE);
}

using TupleDataType = int;
class SimpleGPUPipelineStage : public Runtime::Execution::ExecutablePipelineStage {
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Prepare a simple CUDA kernel which adds 42 to the recordValue and then write it to the result
        const char* const SimpleKernel_cu =
            "SimpleKernel.cu\n"
            "__global__ void simpleAdditionKernel(const int* recordValue, const int count, int* result) {\n"
            "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
            "\n"
            "    if (i < count) {\n"
            "        result[i] = recordValue[i] + 42;\n"
            "    }\n"
            "}\n";

        // setup the kernel program and allocate gpu buffer
        cudaKernelWrapper.setup(SimpleKernel_cu, NUMBER_OF_TUPLE * sizeof(TupleDataType));

        return ExecutablePipelineStage::setup(pipelineExecutionContext);
    }

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto inputRecords = buffer.getBuffer<TupleDataType>();

        // obtain an output buffer
        auto outputBuffer = wc.allocateTupleBuffer();
        auto outputRecords = outputBuffer.getBuffer<TupleDataType>();

        // in this test, the kernel return the same number of tuples
        auto numberOfOutputTuples = buffer.getNumberOfTuples();
        outputBuffer.setNumberOfTuples(numberOfOutputTuples);

        // execute the kernel
        cudaKernelWrapper.execute(inputRecords,
                                  buffer.getNumberOfTuples(),
                                  outputRecords,
                                  numberOfOutputTuples,
                                  "simpleAdditionKernel");

        ctx.emitBuffer(outputBuffer, wc);
        return ExecutionResult::Ok;
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // deallocate GPU memory
        cudaKernelWrapper.clean();

        return ExecutablePipelineStage::stop(pipelineExecutionContext);
    }

    CUDAKernelWrapper<TupleDataType, TupleDataType> cudaKernelWrapper;
};

class MultifieldGPUPipelineStage : public Runtime::Execution::ExecutablePipelineStage {
    class InputRecord {
      public:
        [[maybe_unused]] int64_t id;
        [[maybe_unused]] int64_t one;
        [[maybe_unused]] int64_t value;
    };

    class OutputRecord {
      public:
        [[maybe_unused]] int64_t id;
        [[maybe_unused]] int64_t one;
        [[maybe_unused]] int64_t value;
    };

  public:
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Prepare a simple CUDA kernel which adds 42 to the record.value and then write it to the result
        const char* const MultifieldKernel_cu = "MultifieldKernel_cu.cu\n"
                                                "#include \"nes-core/tests/UnitTests/Query/GPUInputRecord.cuh\"\n"
                                                "__global__ void additionKernelMultipleFields(const InputRecord* recordValue, "
                                                "const int count, InputRecord* result) {\n"
                                                "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
                                                "\n"
                                                "    if (i < count) {\n"
                                                "        result[i].id = recordValue[i].id;\n"
                                                "        result[i].one = recordValue[i].one;\n"
                                                "        result[i].value = recordValue[i].value + 42;\n"
                                                "    }\n"
                                                "}\n";

        const char* const header = "nes-core/tests/UnitTests/Query/GPUInputRecord.cuh\n"
                                   "#ifndef NES_GPUINPUTRECORD_CUH\n"
                                   "#define NES_GPUINPUTRECORD_CUH\n"
                                   "\n"
                                   "#include <cstdint>"
                                   "\n"
                                   "class InputRecord {\n"
                                   "    public:\n"
                                   "        int64_t id;\n"
                                   "        int64_t one;\n"
                                   "        int64_t value;\n"
                                   "};\n"
                                   "\n"
                                   "#endif//NES_GPUINPUTRECORD_CUH\n";

        // setup the kernel program and allocate gpu buffer
        cudaKernelWrapper.setup(MultifieldKernel_cu, NUMBER_OF_TUPLE * sizeof(InputRecord), {header});

        return ExecutablePipelineStage::setup(pipelineExecutionContext);
    }

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {
        auto record = buffer.getBuffer<InputRecord>();

        // obtain an output buffer
        auto outputBuffer = wc.allocateTupleBuffer();
        auto outputRecords = outputBuffer.getBuffer<OutputRecord>();

        // in this test, the kernel return the same number of tuples
        auto numberOfOutputTuples = buffer.getNumberOfTuples();
        outputBuffer.setNumberOfTuples(numberOfOutputTuples);

        // execute the kernel
        cudaKernelWrapper.execute(record,
                                  buffer.getNumberOfTuples(),
                                  outputRecords,
                                  numberOfOutputTuples,
                                  "additionKernelMultipleFields");

        ctx.emitBuffer(outputBuffer, wc);
        return ExecutionResult::Ok;
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // deallocate GPU memory
        cudaKernelWrapper.clean();

        return ExecutablePipelineStage::stop(pipelineExecutionContext);
    }

    CUDAKernelWrapper<InputRecord, OutputRecord> cudaKernelWrapper;
};

// Test offloading only the column to be offloaded to the gpu
class ColumnLayoutGPUPipelineStage : public Runtime::Execution::ExecutablePipelineStage {
    class InputRecord {
      public:
        [[maybe_unused]] int64_t id;
        [[maybe_unused]] int64_t one;
        [[maybe_unused]] int64_t value;
    };

  public:
    uint32_t setup(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // Prepare a simple CUDA kernel which adds 42 to the record and then write it to the result
        const char* const ColumnLayoutKernel_cu = "ColumnLayoutKernel_cu.cu\n"
                                                  "#include \"nes-core/tests/UnitTests/Query/GPUInputRecord.cuh\"\n"
                                                  "__global__ void additionKernelColumnLayout(const int64_t* recordValue, "
                                                  "const int count, int64_t* result) {\n"
                                                  "    auto i = blockIdx.x * blockDim.x + threadIdx.x;\n"
                                                  "\n"
                                                  "    if (i < count) {\n"
                                                  "        result[i] = recordValue[i] + 42;\n"
                                                  "    }\n"
                                                  "}\n";

        const char* const header = "nes-core/tests/UnitTests/Query/GPUInputRecord.cuh\n"
                                   "#ifndef NES_GPUINPUTRECORD_CUH\n"
                                   "#define NES_GPUINPUTRECORD_CUH\n"
                                   "\n"
                                   "#include <cstdint>"
                                   "\n"
                                   "class InputRecord {\n"
                                   "    public:\n"
                                   "        int64_t id;\n"
                                   "        int64_t one;\n"
                                   "        int64_t value;\n"
                                   "};\n"
                                   "\n"
                                   "#endif//NES_GPUINPUTRECORD_CUH\n";
        // setup the kernel program and allocate gpu buffer
        // setup the kernel program and allocate gpu buffer
        cudaKernelWrapper.setup(ColumnLayoutKernel_cu, NUMBER_OF_TUPLE * sizeof(InputRecord), {header});
        //        cudaKernelWrapper.setup(ColumnLayoutKernel_cu, NUMBER_OF_TUPLE * sizeof(int64_t));

        // define the schema (to be used to create column layout and obtaining column offset)
        testSchemaColumnLayout = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)
                                     ->addField("test$id", BasicType::INT64)
                                     ->addField("test$one", BasicType::INT64)
                                     ->addField("test$value", BasicType::INT64);

        return ExecutablePipelineStage::setup(pipelineExecutionContext);
    }

    ExecutionResult execute(Runtime::TupleBuffer& buffer,
                            Runtime::Execution::PipelineExecutionContext& ctx,
                            Runtime::WorkerContext& wc) override {

        // obtain the column offset
        auto columnLayout = NES::Runtime::MemoryLayouts::ColumnLayout::create(testSchemaColumnLayout, buffer.getBufferSize());
        auto valColOffset = columnLayout->getColumnOffsets()[2];// test$value is column 2

        // take the part of buffer in the specified offset
        auto valueBuffer = reinterpret_cast<int64_t*>(buffer.getBuffer() + valColOffset);

        // obtain an output buffer
        auto outputBuffer = wc.allocateTupleBuffer();
        auto outputRecords = outputBuffer.getBuffer<int64_t>();

        // in this test, the kernel return the same number of tuples
        auto numberOfOutputTuples = buffer.getNumberOfTuples();
        outputBuffer.setNumberOfTuples(numberOfOutputTuples);

        // execute the kernel
        cudaKernelWrapper.execute(valueBuffer,
                                  buffer.getNumberOfTuples(),
                                  outputRecords,
                                  numberOfOutputTuples,
                                  "additionKernelColumnLayout");

        ctx.emitBuffer(outputBuffer, wc);
        return ExecutionResult::Ok;
    }

    uint32_t stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override {
        // deallocate GPU memory
        cudaKernelWrapper.clean();

        return ExecutablePipelineStage::stop(pipelineExecutionContext);
    }

    CUDAKernelWrapper<int64_t, int64_t> cudaKernelWrapper;
    SchemaPtr testSchemaColumnLayout;
};

// Test the execution of an external operator using a simple GPU Kernel from a source of simple integer
TEST_F(GPUQueryExecutionTest, GPUOperatorSimpleQuery) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchemaSimple,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchemaSimple,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 0,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("value", BasicType::INT32);
    auto testSink = std::make_shared<TestSink>(NUMBER_OF_TUPLE, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("value") < 5).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // add physical operator behind the filter
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];

    auto customPipelineStage = std::make_shared<SimpleGPUPipelineStage>();
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 2u);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchemaSimple, nodeEngine->getBufferManager()->getBufferSize());
        fillBufferToSimpleSchema(buffer, memoryLayout);
        plan->setup();
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
        ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
        ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

        auto valueField = Runtime::MemoryLayouts::RowLayoutField<int32_t, true>::create(0, memoryLayout, resultBuffer);
        for (int recordIndex = 0; recordIndex < 5; ++recordIndex) {
            // id
            EXPECT_EQ(valueField[recordIndex], recordIndex + 42);
        }
    } else {
        FAIL();
    }

    cleanUpPlan(plan);
    testSink->cleanupBuffers();
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// Test the execution of an external operator on a source with a custom structure
TEST_F(GPUQueryExecutionTest, GPUOperatorWithMultipleFields) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchemaMultipleFields,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchemaMultipleFields,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 0,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create()->addField("id", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(NUMBER_OF_TUPLE, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // add physical operator behind the filter
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];

    auto customPipelineStage = std::make_shared<MultifieldGPUPipelineStage>();
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 2u);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::RowLayout::create(testSchemaMultipleFields, nodeEngine->getBufferManager()->getBufferSize());
        fillBufferToMultiFieldSchema(buffer, memoryLayout);
        plan->setup();
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
        ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
        ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

        auto resultRecordIndexFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(0, memoryLayout, resultBuffer);
        auto resultRecordValueFields =
            Runtime::MemoryLayouts::RowLayoutField<int64_t, true>::create(2, memoryLayout, resultBuffer);
        for (uint32_t recordIndex = 0u; recordIndex < 5u; ++recordIndex) {
            // id
            EXPECT_EQ(resultRecordIndexFields[recordIndex], recordIndex);
            EXPECT_EQ(resultRecordValueFields[recordIndex], (recordIndex % 2) + 42);
        }
    } else {
        FAIL();
    }

    cleanUpPlan(plan);
    testSink->cleanupBuffers();
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}

// Test the execution of an external operator on a source with column layout
TEST_F(GPUQueryExecutionTest, GPUOperatorOnColumnLayout) {
    // creating query plan
    auto testSourceDescriptor = std::make_shared<TestUtils::TestSourceDescriptor>(
        testSchemaColumnLayout,
        [&](OperatorId id,
            const SourceDescriptorPtr&,
            const Runtime::NodeEnginePtr&,
            size_t numSourceLocalBuffers,
            std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors) -> DataSourcePtr {
            return createDefaultDataSourceWithSchemaForOneBuffer(testSchemaColumnLayout,
                                                                 nodeEngine->getBufferManager(),
                                                                 nodeEngine->getQueryManager(),
                                                                 id,
                                                                 0,
                                                                 numSourceLocalBuffers,
                                                                 std::move(successors));
        });

    auto outputSchema = Schema::create(Schema::MemoryLayoutType::COLUMNAR_LAYOUT)->addField("value", BasicType::INT64);
    auto testSink = std::make_shared<TestSink>(NUMBER_OF_TUPLE, outputSchema, nodeEngine);
    auto testSinkDescriptor = std::make_shared<TestUtils::TestSinkDescriptor>(testSink);

    auto query = TestQuery::from(testSourceDescriptor).filter(Attribute("id") < 5).sink(testSinkDescriptor);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    // add physical operator behind the filter
    auto filterOperator = queryPlan->getOperatorByType<FilterLogicalOperatorNode>()[0];

    auto customPipelineStage = std::make_shared<ColumnLayoutGPUPipelineStage>();
    auto externalOperator =
        NES::QueryCompilation::PhysicalOperators::PhysicalExternalOperator::create(SchemaPtr(), SchemaPtr(), customPipelineStage);

    filterOperator->insertBetweenThisAndParentNodes(externalOperator);

    auto request = QueryCompilation::QueryCompilationRequest::create(queryPlan, nodeEngine);
    auto queryCompiler = TestUtils::createTestQueryCompiler();
    auto result = queryCompiler->compileQuery(request);
    auto plan = result->getExecutableQueryPlan();
    // The plan should have one pipeline
    ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Created);
    EXPECT_EQ(plan->getPipelines().size(), 2u);
    Runtime::WorkerContext workerContext{1, nodeEngine->getBufferManager(), 4};
    if (auto buffer = nodeEngine->getBufferManager()->getBufferBlocking(); !!buffer) {
        auto memoryLayout =
            Runtime::MemoryLayouts::ColumnLayout::create(testSchemaColumnLayout, nodeEngine->getBufferManager()->getBufferSize());
        fillBufferColumnLayout(buffer, memoryLayout);
        plan->setup();
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Deployed);
        ASSERT_TRUE(plan->start(nodeEngine->getStateManager()));
        ASSERT_EQ(plan->getStatus(), Runtime::Execution::ExecutableQueryPlanStatus::Running);
        ASSERT_EQ(plan->getPipelines()[1]->execute(buffer, workerContext), ExecutionResult::Ok);

        // This plan should produce one output buffer
        EXPECT_EQ(testSink->getNumberOfResultBuffers(), 1u);

        auto resultBuffer = testSink->get(0);
        // The output buffer should contain 5 tuple;
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5u);

        // The result only contains a single column
        auto resultRecordValueFields =
            Runtime::MemoryLayouts::ColumnLayoutField<int64_t, true>::create(0, memoryLayout, resultBuffer);
        for (uint32_t recordIndex = 0u; recordIndex < 5u; ++recordIndex) {
            // id
            EXPECT_EQ(resultRecordValueFields[recordIndex], (recordIndex % 2) + 42);
            NES_DEBUG("expected: " << (recordIndex % 2) + 42 << " actual: " << resultRecordValueFields[recordIndex]);
        }
    } else {
        FAIL();
    }

    cleanUpPlan(plan);
    testSink->cleanupBuffers();
    ASSERT_EQ(testSink->getNumberOfResultBuffers(), 0U);
}
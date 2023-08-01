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

#include <API/QueryAPI.hpp>
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Network/NetworkChannel.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/CCodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/CodeGenerator/GeneratedCode.hpp>
#include <QueryCompiler/CodeGenerator/LegacyExpression.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableDataType.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableSumAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/ColumnLayoutField.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutField.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/ExecutableCompleteAggregationTriggerAction.hpp>
#include <Windowing/WindowAggregations/ExecutableSumAggregation.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <cassert>
#include <cmath>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

#include <API/Expressions/Expressions.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelOperatorHandler.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/CEPOperatorHandler/CEPOperatorHandler.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategy.hpp>
#include <Windowing/WindowActions/CompleteAggregationTriggerActionDescriptor.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowHandler/AggregationWindowHandler.hpp>
#include <Windowing/WindowHandler/JoinHandler.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
using std::cout;
using std::endl;
using namespace NES::Runtime;
namespace NES {

class OperatorCodeGenerationTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OperatorOperatorCodeGenerationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup OperatorOperatorCodeGenerationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES_DEBUG("Setup OperatorOperatorCodeGenerationTest test case.");
        Testing::NESBaseTest::SetUp();
        dataPort = Testing::NESBaseTest::getAvailablePort();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        listener = std::make_shared<DummyQueryListener>();
    }

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_DEBUG("Tear down OperatorOperatorCodeGenerationTest test case.");
        dataPort.reset();
        Testing::NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_DEBUG("Tear down OperatorOperatorCodeGenerationTest test class."); }

  protected:
    Testing::BorrowedPortPtr dataPort;
    AbstractQueryStatusListenerPtr listener;
};

class TestPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    TestPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, Runtime::Execution::OperatorHandlerPtr operatorHandler)
        : TestPipelineExecutionContext(std::move(queryManager),
                                       std::vector<Runtime::Execution::OperatorHandlerPtr>{std::move(operatorHandler)}) {}
    TestPipelineExecutionContext(Runtime::QueryManagerPtr queryManager,
                                 std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            queryManager->getBufferManager(),
            queryManager->getNumberOfWorkerThreads(),
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->buffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->buffers.emplace_back(std::move(buffer));
            },
            std::move(operatorHandlers)){
            // nop
        };

    ~TestPipelineExecutionContext() { buffers.clear(); }

    std::vector<TupleBuffer> buffers;
};

DataSourcePtr createTestSourceCodeGen(const Runtime::BufferManagerPtr& bPtr, const Runtime::QueryManagerPtr& dPtr) {
    return std::make_shared<DefaultSource>(Schema::create()->addField("campaign_id", DataTypeFactory::createInt64()),
                                           bPtr,
                                           dPtr,
                                           1,
                                           1,
                                           1,
                                           0,
                                           12);
}

class SelectionDataGenSource : public GeneratorSource {
  public:
    SelectionDataGenSource(SchemaPtr schema,
                           Runtime::BufferManagerPtr bPtr,
                           Runtime::QueryManagerPtr dPtr,
                           const uint64_t pNum_buffers_to_process)
        : GeneratorSource(std::move(schema),
                          std::move(bPtr),
                          std::move(dPtr),
                          pNum_buffers_to_process,
                          1,
                          0,
                          12,
                          GatheringMode::INTERVAL_MODE,
                          {}) {}

    ~SelectionDataGenSource() override = default;

    std::optional<Runtime::TupleBuffer> receiveData() override {
        // 10 tuples of size one
        auto buffer = allocateBuffer();
        for (uint32_t i = 0; i < buffer.getCapacity(); i++) {
            buffer[i]["id"].write<uint32_t>(i);
            buffer[i]["value"].write<uint32_t>(i * 2);
            auto charArray = buffer[i]["text"].read<char*>();
            for (std::size_t j = 0; j < 11; ++j) {
                charArray[j] = ((j + i) % (255 - 'a')) + 'a';
            }
            charArray[11] = '\0';
        }
        buffer.setNumberOfTuples(buffer.getCapacity());
        return buffer.getBuffer();
    }

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        uint32_t value;
        char text[12];
    };
};

DataSourcePtr createTestSourceCodeGenFilter(const Runtime::BufferManagerPtr& bPtr, const Runtime::QueryManagerPtr& dPtr) {
    DataSourcePtr source(std::make_shared<SelectionDataGenSource>(Schema::create()
                                                                      ->addField("id", DataTypeFactory::createUInt32())
                                                                      ->addField("value", DataTypeFactory::createUInt32())
                                                                      ->addField("text", DataTypeFactory::createFixedChar(12)),
                                                                  bPtr,
                                                                  dPtr,
                                                                  1));

    return source;
}

class PredicateTestingDataGeneratorSource : public GeneratorSource {
  public:
    PredicateTestingDataGeneratorSource(SchemaPtr schema,
                                        Runtime::BufferManagerPtr bPtr,
                                        Runtime::QueryManagerPtr dPtr,
                                        const uint64_t pNum_buffers_to_process)
        : GeneratorSource(std::move(schema),
                          std::move(bPtr),
                          std::move(dPtr),
                          pNum_buffers_to_process,
                          1,
                          0,
                          12,
                          GatheringMode::INTERVAL_MODE,
                          {}) {}

    ~PredicateTestingDataGeneratorSource() override = default;

    struct __attribute__((packed)) InputTuple {
        uint32_t id;
        [[maybe_unused]] int16_t valueSmall;
        [[maybe_unused]] float valueFloat;
        [[maybe_unused]] double valueDouble;
        char singleChar;
        char text[12];
    };

    std::optional<TupleBuffer> receiveData() override {
        auto buffer = allocateBuffer();
        for (uint32_t i = 0; i < buffer.getCapacity(); i++) {
            // id
            buffer[i][0].write<uint32_t>(i);
            // valueSmall
            buffer[i][1].write<int16_t>(-123 + (i * 2));
            // valueFloat
            buffer[i][2].write<float>(i * M_PI);
            // valueDouble
            buffer[i][3].write<double>(i * M_PI * 2);
            // singleChar
            buffer[i][4].write<char>(((i + 1) % (127 - 'A')) + 'A');
            //text
            auto charArray = buffer[i][5].read<char*>();
            for (std::size_t j = 0; j < 11; ++j) {
                charArray[j] = ((i + 1) % 64) + 64;// text
            }
            charArray[11] = '\0';
        }
        buffer.setNumberOfTuples(buffer.getCapacity());
        return buffer.getBuffer();
    }
};

DataSourcePtr createTestSourceCodeGenPredicate(const Runtime::BufferManagerPtr& bPtr,
                                               const Runtime::QueryManagerPtr& dPtr,
                                               Schema::MemoryLayoutType layoutType = Schema::ROW_LAYOUT) {
    DataSourcePtr source(
        std::make_shared<PredicateTestingDataGeneratorSource>(Schema::create(layoutType)
                                                                  ->addField("id", DataTypeFactory::createUInt32())
                                                                  ->addField("valueSmall", DataTypeFactory::createInt16())
                                                                  ->addField("valueFloat", DataTypeFactory::createFloat())
                                                                  ->addField("valueDouble", DataTypeFactory::createDouble())
                                                                  ->addField("valueChar", DataTypeFactory::createChar())
                                                                  ->addField("text", DataTypeFactory::createFixedChar(12)),
                                                              bPtr,
                                                              dPtr,
                                                              1));

    return source;
}

class WindowTestingDataGeneratorSource : public GeneratorSource {
  public:
    WindowTestingDataGeneratorSource(SchemaPtr schema,
                                     Runtime::BufferManagerPtr bPtr,
                                     Runtime::QueryManagerPtr dPtr,
                                     const uint64_t pNum_buffers_to_process)
        : GeneratorSource(std::move(schema),
                          std::move(bPtr),
                          std::move(dPtr),
                          pNum_buffers_to_process,
                          1,
                          0,
                          12,
                          GatheringMode::INTERVAL_MODE,
                          {}) {}

    ~WindowTestingDataGeneratorSource() override = default;

    struct __attribute__((packed)) InputTuple {
        uint64_t key;
        uint64_t value;
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = 10;

        assert(buf.getBuffer() != nullptr);

        auto* tuples = (InputTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].key = i % 2;
            tuples[i].value = 1;
        }

        //bufbufferSizeInBytes=(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }

    void runningRoutine() override {
        // nop
    }
};

class WindowTestingWindowGeneratorSource : public GeneratorSource {
  public:
    WindowTestingWindowGeneratorSource(SchemaPtr schema,
                                       Runtime::BufferManagerPtr bPtr,
                                       Runtime::QueryManagerPtr dPtr,
                                       const uint64_t pNum_buffers_to_process)
        : GeneratorSource(std::move(schema),
                          std::move(bPtr),
                          std::move(dPtr),
                          pNum_buffers_to_process,
                          1,
                          0,
                          12,
                          GatheringMode::INTERVAL_MODE,
                          {}) {}

    ~WindowTestingWindowGeneratorSource() override = default;

    struct __attribute__((packed)) WindowTuple {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t value;
    };

    std::optional<TupleBuffer> receiveData() override {
        // 10 tuples of size one
        TupleBuffer buf = bufferManager->getBufferBlocking();
        uint64_t tupleCnt = 10;

        assert(buf.getBuffer() != nullptr);

        auto* tuples = (WindowTuple*) buf.getBuffer();

        for (uint32_t i = 0; i < tupleCnt; i++) {
            tuples[i].start = i;
            tuples[i].end = i * 2;
            tuples[i].key = 1;
            tuples[i].value = 1;
        }

        //bufbufferSizeInBytes=(sizeof(InputTuple));
        buf.setNumberOfTuples(tupleCnt);
        return buf;
    }

    void runningRoutine() override {
        // nop
    }
};

DataSourcePtr createWindowTestDataSource(const Runtime::BufferManagerPtr& bPtr, const Runtime::QueryManagerPtr& dPtr) {
    DataSourcePtr source(
        std::make_shared<WindowTestingDataGeneratorSource>(Schema::create()
                                                               ->addField("window$key", DataTypeFactory::createUInt64())
                                                               ->addField("window$value", DataTypeFactory::createUInt64()),
                                                           bPtr,
                                                           dPtr,
                                                           10));
    return source;
}

DataSourcePtr createWindowTestSliceSource(const Runtime::BufferManagerPtr& bPtr,
                                          const Runtime::QueryManagerPtr& dPtr,
                                          const SchemaPtr& schema) {
    DataSourcePtr source(std::make_shared<WindowTestingWindowGeneratorSource>(schema, bPtr, dPtr, 10));
    return source;
}

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType, class sumType>
std::shared_ptr<Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>>
createWindowHandler(const Windowing::LogicalWindowDefinitionPtr& windowDefinition, const SchemaPtr& resultSchema) {

    auto aggregation = sumType::create();
    auto trigger = Windowing::ExecutableOnTimeTriggerPolicy::create(1000);
    auto triggerAction =
        Windowing::ExecutableCompleteAggregationTriggerAction<uint64_t, uint64_t, uint64_t, uint64_t>::create(windowDefinition,
                                                                                                              aggregation,
                                                                                                              resultSchema,
                                                                                                              1,
                                                                                                              0);
    return Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>::create(windowDefinition,
                                                                                               aggregation,
                                                                                               trigger,
                                                                                               triggerAction,
                                                                                               1,
                                                                                               0);
}

/**
 * @brief This test generates a simple copy function, which copies code from one buffer to another
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCopy) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();
    auto source = createTestSourceCodeGen(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    NES_INFO("Generate Code");
    /* generate code for scanning input buffer */
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context);
    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(Schema::create()->addField("campaign_id", DataTypeFactory::createUInt64()),
                                       QueryCompilation::NO_OPTIMIZATION,
                                       QueryCompilation::FIELD_COPY,
                                       context);
    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input and output tuple buffer */
    auto schema = Schema::create()->addField("i64", DataTypeFactory::createUInt64());
    source->open();
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    stage->setup(*queryContext);
    stage->start(*queryContext);
    if (auto buffer = source->receiveData().value(); !!buffer) {
        /* execute Stage */
        NES_INFO("Processing " << buffer.getNumberOfTuples() << " tuples: ");
        stage->execute(buffer, *queryContext, wctx);
        if (auto resultBuffer = queryContext->buffers[0]; !!resultBuffer) {
            /* check for correctness, input source produces uint64_t tuples and stores a 1 in each tuple */
            EXPECT_EQ(buffer.getNumberOfTuples(), resultBuffer.getNumberOfTuples());
            auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, nodeEngine->getBufferManager()->getBufferSize());
            auto firstFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(0, layout, resultBuffer);
            for (uint64_t recordIndex = 0; recordIndex < buffer.getNumberOfTuples(); ++recordIndex) {
                EXPECT_EQ(firstFields[recordIndex], 1UL);
            }
        }
    }
    queryContext.reset();
}

/**
 * @brief This test generates a predicate, which filters elements in the input buffer
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationFilterPredicate) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    auto source = createTestSourceCodeGenFilter(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();

    /* generate code for scanning input buffer */
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context);

    auto pred = std::dynamic_pointer_cast<QueryCompilation::Predicate>(
        (QueryCompilation::PredicateItem(inputSchema->get(0))
         < QueryCompilation::PredicateItem(DataTypeFactory::createBasicValue(DataTypeFactory::createInt64(), "5")))
            .copy());

    codeGenerator->generateCodeForFilter(pred, context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(source->getSchema(),
                                       QueryCompilation::NO_OPTIMIZATION,
                                       QueryCompilation::FIELD_COPY,
                                       context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();
    NES_INFO("Processing " << inputBuffer.getNumberOfTuples() << " tuples: ");

    /* execute Stage */
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);
    auto resultBuffer = queryContext->buffers[0];
    /* check for correctness, input source produces tuples consisting of two uint32_t values, 5 values will match the predicate */
    NES_INFO("Number of generated output tuples: " << resultBuffer.getNumberOfTuples());
    EXPECT_EQ(resultBuffer.getNumberOfTuples(), 5U);

    auto* resultData = (SelectionDataGenSource::InputTuple*) resultBuffer.getBuffer();
    for (uint64_t i = 0; i < 5; ++i) {
        EXPECT_EQ(resultData[i].id, i);
        EXPECT_EQ(resultData[i].value, i * 2);
    }
    queryContext.reset();
}

TEST_F(OperatorCodeGenerationTest, codeGenerationScanOperator) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationWindowAssigner) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);

    auto trigger = Windowing::OnTimeTriggerPolicyDescription::create(1000);

    auto sum = Windowing::SumAggregationDescriptor::on(Attribute("window$value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = Windowing::LogicalWindowDefinition::create(
        {Attribute("window$key", BasicType::UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
        {sum},
        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(),
        trigger,
        triggerAction,
        0);

    auto strategy = EventTimeWatermarkStrategy::create(windowDefinition->getKeys()[0], 12, 1);
    codeGenerator->generateCodeForWatermarkAssigner(strategy, context1);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    context2->pipelineName = "1";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);
}

/**
 * @brief This test generates a window slicer
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationDistributedSlicer) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();
    context1->pipelineName = "1";
    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    auto trigger = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto sum = SumAggregationDescriptor::on(Attribute("window$value", BasicType::UINT64));
    auto windowDefinition = LogicalWindowDefinition::create(
        {Attribute("window$key", BasicType::UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
        {sum},
        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(),
        trigger,
        triggerAction,
        0);
    windowDefinition->setNumberOfInputEdges(1);
    auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
    codeGenerator->generateCodeForSlicingWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField("window$key", UINT64)
                                  ->addField("window$value", UINT64);
    // init window handler
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition,
            windowOutputSchema);
    windowHandler->start(nodeEngine->getStateManager(), 0);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);

    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    stage1->setup(*executionContext);
    stage1->start(*executionContext);
    stage1->execute(inputBuffer, *executionContext, wctx);

    //check partial aggregates in window state
    auto* stateVar = windowHandler->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5UL);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5UL);
    windowHandler->stop();
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationDistributedCombiner) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    auto schema = Schema::create()
                      ->addField(createField("window$start", UINT64))
                      ->addField(createField("window$end", UINT64))
                      ->addField(createField("window$cnt", UINT64))
                      ->addField(createField("key", UINT64))
                      ->addField("value", UINT64);

    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();
    context1->pipelineName = "1";
    codeGenerator->generateCodeForScan(schema, schema, context1);
    auto trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("value", UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDefinition =
        LogicalWindowDefinition::create({Attribute("key", UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
                                        {sum},
                                        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)),
                                        DistributionCharacteristic::createCompleteWindowType(),
                                        trigger,
                                        triggerAction,
                                        0);
    windowDefinition->setNumberOfInputEdges(1);
    auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
    codeGenerator->generateCodeForCombiningWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(schema, schema, context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("window$start", UINT64))
                                  ->addField(createField("window$end", UINT64))
                                  ->addField("window$key", UINT64)
                                  ->addField("value", UINT64);

    // init window handler
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition,
            windowOutputSchema);
    windowHandler->start(nodeEngine->getStateManager(), 0);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);
    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);

    windowHandler->setup(executionContext);

    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    {
        Runtime::MemoryLayouts::RowLayoutPtr rowLayout =
            Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());

        auto startFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(0, rowLayout, buffer);
        auto stopFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(1, rowLayout, buffer);
        auto cntFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(2, rowLayout, buffer);
        auto keyFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(3, rowLayout, buffer);
        auto valueFields = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(4, rowLayout, buffer);

        startFields[0] = 100;//start 100
        stopFields[0] = 110; //stop 200
        cntFields[0] = 1;    //cnt
        keyFields[0] = 1;    //key 1
        valueFields[0] = 10; //value 10
        buffer.setNumberOfTuples(1);

        startFields[1] = 100;//start 100
        stopFields[1] = 110; //stop 200
        cntFields[0] = 1;    //cnt
        keyFields[1] = 1;    //key 1
        valueFields[1] = 8;  //value 8
        buffer.setNumberOfTuples(2);

        startFields[2] = 100;//start 100
        stopFields[2] = 110; //stop 200
        cntFields[0] = 1;    //cnt
        keyFields[2] = 1;    //key 1
        valueFields[2] = 2;  //value 10
        buffer.setNumberOfTuples(3);

        startFields[3] = 200;//start 200
        stopFields[3] = 210; //stop 210
        cntFields[0] = 1;    //cnt
        keyFields[3] = 3;    //key 3
        valueFields[3] = 2;  //value 10
        buffer.setNumberOfTuples(4);

        startFields[4] = 200;//start 200
        stopFields[4] = 210; //stop 210
        cntFields[0] = 1;    //cnt
        keyFields[4] = 5;    //key 1
        valueFields[4] = 12; //value 12
        buffer.setNumberOfTuples(5);
    }
    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(schema, buffer.getBufferSize());
    auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, buffer);
    NES_DEBUG("buffer=" << dynamicTupleBuffer);

    /* execute Stage */
    stage1->setup(*executionContext);
    stage1->start(*executionContext);
    stage1->execute(buffer, *executionContext, wctx);

    //check partial aggregates in window state
    auto* stateVar = windowHandler->getTypedWindowState();
    std::vector<uint64_t> results;
    for (auto& [key, val] : stateVar->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        for (auto& slice : val->getSliceMetadata()) {
            NES_DEBUG("start=" << slice.getStartTs() << " end=" << slice.getEndTs());
            results.push_back(slice.getStartTs());
            results.push_back(slice.getEndTs());
        }
        for (auto& agg : val->getPartialAggregates()) {
            NES_DEBUG("key=" << key);
            results.push_back(key);
            NES_DEBUG("value=" << agg);
            results.push_back(agg);
        }
    }

    EXPECT_EQ(results[0], 100ULL);
    EXPECT_EQ(results[1], 110ULL);
    EXPECT_EQ(results[2], 1ULL);
    EXPECT_EQ(results[3], 20ULL);

    EXPECT_EQ(results[4], 200ULL);
    EXPECT_EQ(results[5], 210ULL);
    EXPECT_EQ(results[6], 3ULL);
    EXPECT_EQ(results[7], 2ULL);

    EXPECT_EQ(results[8], 200ULL);
    EXPECT_EQ(results[9], 210ULL);
    EXPECT_EQ(results[10], 5ULL);
    EXPECT_EQ(results[11], 12ULL);
    windowHandler->stop();
}

TEST_F(OperatorCodeGenerationTest, codeGenerationTriggerWindowOnRecord) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    auto schema = Schema::create()
                      ->addField(createField("window$start", UINT64))
                      ->addField(createField("window$end", UINT64))
                      ->addField(createField("window$cnt", UINT64))
                      ->addField(createField("window$key", UINT64))
                      ->addField("value", UINT64);

    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();
    context1->pipelineName = "1";
    codeGenerator->generateCodeForScan(schema, schema, context1);
    auto trigger = OnRecordTriggerPolicyDescription::create();

    auto sum = SumAggregationDescriptor::on(Attribute("value", UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();

    auto windowDefinition =
        LogicalWindowDefinition::create({Attribute("window$key", UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
                                        {sum},
                                        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)),
                                        DistributionCharacteristic::createCompleteWindowType(),
                                        trigger,
                                        triggerAction,
                                        0);
    windowDefinition->setNumberOfInputEdges(1);
    auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
    codeGenerator->generateCodeForCombiningWindow(windowDefinition, aggregate, context1, 0);
    std::string codeString = codeGenerator->generateCode(context1);

    auto found = codeString.find("windowHandler->trigger();");
    cout << "code=" << codeString << std::endl;
    EXPECT_NE(found, std::string::npos);
}

/**
 * @brief This test generates a predicate with string comparision
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationStringComparePredicateTest) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    codeGenerator->generateCodeForScan(inputSchema, inputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForFilter(
        QueryCompilation::createPredicate((inputSchema->get(2) > QueryCompilation::PredicateItem(30.4))
                                          && (inputSchema->get(4) == QueryCompilation::PredicateItem('F')
                                              || (inputSchema->get(5) == QueryCompilation::PredicateItem("HHHHHHHHHHH")))),
        context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(inputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto optVal = source->receiveData();
    ASSERT_TRUE(optVal.has_value());
    if (auto inputBuffer = *optVal; !!inputBuffer) {

        /* execute Stage */

        auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                           std::vector<Runtime::Execution::OperatorHandlerPtr>());
        Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
        stage->setup(*queryContext);
        stage->start(*queryContext);
        stage->execute(inputBuffer, *queryContext, wctx);

        auto resultBuffer = queryContext->buffers[0];

        /* check for correctness, input source produces tuples consisting of two uint32_t values, 3 values will match the predicate */
        EXPECT_EQ(resultBuffer.getNumberOfTuples(), 3UL);
        queryContext.reset();
    }
}
#ifdef TFDEF
/**
 * @brief This test generates an infer model operator
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationInferModelTest) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inferModelOperatorHandler =
        InferModel::InferModelOperatorHandler::create(std::string(TEST_DATA_DIRECTORY) + "iris_95acc.tflite");
    context->registerOperatorHandler(inferModelOperatorHandler);

    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());
    auto iris0 = AttributeField::create("iris0", DataTypeFactory::createFloat());
    auto iris1 = AttributeField::create("iris1", DataTypeFactory::createFloat());
    auto iris2 = AttributeField::create("iris2", DataTypeFactory::createFloat());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create()
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(iris0)
                            ->addField(iris1)
                            ->addField(iris2)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    auto valF = std::make_shared<ExpressionItem>(Attribute("valueFloat", NES::BasicType::FLOAT32));
    auto prediction0 = std::make_shared<ExpressionItem>(Attribute("iris0"));
    auto prediction1 = std::make_shared<ExpressionItem>(Attribute("iris1"));
    auto predection2 = std::make_shared<ExpressionItem>(Attribute("iris2"));
    auto op = LogicalOperatorFactory::createInferModelOperator(std::string(TEST_DATA_DIRECTORY) + "iris_95acc.tflite",
                                                               {valF, valF, valF, valF},
                                                               {prediction0, prediction1, predection2});
    auto imop = op->as<InferModel::InferModelLogicalOperatorNode>();

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);
    codeGenerator->generateInferModelSetup(context, inferModelOperatorHandler);
    codeGenerator->generateCodeForInferModel(context, imop->getInputFields(), imop->getOutputFields());

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 46};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), inferModelOperatorHandler);

    stage->setup(*queryContext.get());
    stage->start(*queryContext.get());
    context->getOperatorHandlers()[0]->start(queryContext, nodeEngine->getStateManager(), 0);
    stage->execute(inputBuffer, *queryContext.get(), wctx);
}
#endif
/**
 * @brief This test generates a map predicate, which manipulates the input buffer content
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationMapPredicateTest) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create()
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(mappedValue,
                                      createPredicate((inputSchema->get(2) * QueryCompilation::PredicateItem(inputSchema->get(3)))
                                                      + QueryCompilation::PredicateItem(2)),
                                      context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];

    auto inputLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto outputLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());

    auto secondFieldsInput = Runtime::MemoryLayouts::RowLayoutField<float, true>::create(2, inputLayout, inputBuffer);
    auto thirdFieldsInput = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(3, inputLayout, inputBuffer);

    auto fourthFieldsOutput = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(4, outputLayout, resultBuffer);

    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = secondFieldsInput[recordIndex];
        auto doubleValue = thirdFieldsInput[recordIndex];
        auto reference = (floatValue * doubleValue) + 2;
        auto const mv = fourthFieldsOutput[recordIndex];
        EXPECT_EQ(reference, mv);
    }
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content.
 * This one with a column layout for both input and output schema
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationMapPredicateTestColLayout) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source =
        createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), Schema::COLUMNAR_LAYOUT);
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create(Schema::COLUMNAR_LAYOUT)
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(mappedValue,
                                      createPredicate((inputSchema->get(2) * QueryCompilation::PredicateItem(inputSchema->get(3)))
                                                      + QueryCompilation::PredicateItem(2)),
                                      context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];
    auto inputLayout = Runtime::MemoryLayouts::ColumnLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto outputLayout =
        Runtime::MemoryLayouts::ColumnLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());

    auto secondFieldsInput = Runtime::MemoryLayouts::ColumnLayoutField<float, true>::create(2, inputLayout, inputBuffer);
    auto thirdFieldsInput = Runtime::MemoryLayouts::ColumnLayoutField<double, true>::create(3, inputLayout, inputBuffer);
    auto fourthFieldsOutput = Runtime::MemoryLayouts::ColumnLayoutField<double, true>::create(4, outputLayout, resultBuffer);

    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = secondFieldsInput[recordIndex];
        auto doubleValue = thirdFieldsInput[recordIndex];
        auto reference = (floatValue * doubleValue) + 2;
        auto const mv = fourthFieldsOutput[recordIndex];
        EXPECT_EQ(reference, mv);
    }
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content.
 * This one with a column layout for input and row for output schema
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationMapPredicateTestColRowLayout) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source =
        createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), Schema::COLUMNAR_LAYOUT);
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create(Schema::ROW_LAYOUT)
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(mappedValue,
                                      createPredicate((inputSchema->get(2) * QueryCompilation::PredicateItem(inputSchema->get(3)))
                                                      + QueryCompilation::PredicateItem(2)),
                                      context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];
    auto inputLayout = Runtime::MemoryLayouts::ColumnLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto outputLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());

    auto secondFieldsInput = Runtime::MemoryLayouts::ColumnLayoutField<float, true>::create(2, inputLayout, inputBuffer);
    auto thirdFieldsInput = Runtime::MemoryLayouts::ColumnLayoutField<double, true>::create(3, inputLayout, inputBuffer);
    auto fourthFieldsOutput = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(4, outputLayout, resultBuffer);

    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = secondFieldsInput[recordIndex];
        auto doubleValue = thirdFieldsInput[recordIndex];
        auto reference = (floatValue * doubleValue) + 2;
        auto const mv = fourthFieldsOutput[recordIndex];
        EXPECT_EQ(reference, mv);
    }
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content.
 * This one with a row layout for input and column for output schema
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationMapPredicateTestRowColLayout) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source =
        createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), Schema::ROW_LAYOUT);
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create(Schema::COLUMNAR_LAYOUT)
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(mappedValue,
                                      createPredicate((inputSchema->get(2) * QueryCompilation::PredicateItem(inputSchema->get(3)))
                                                      + QueryCompilation::PredicateItem(2)),
                                      context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];
    auto inputLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto outputLayout =
        Runtime::MemoryLayouts::ColumnLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());

    auto bindedInputRowLayout = inputLayout->bind(inputBuffer);
    auto bindedOutputColumnLayout = outputLayout->bind(resultBuffer);

    auto secondFieldsInput = Runtime::MemoryLayouts::RowLayoutField<float, true>::create(2, inputLayout, inputBuffer);
    auto thirdFieldsInput = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(3, inputLayout, inputBuffer);
    auto fourthFieldsOutput = Runtime::MemoryLayouts::ColumnLayoutField<double, true>::create(4, outputLayout, resultBuffer);

    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = secondFieldsInput[recordIndex];
        auto doubleValue = thirdFieldsInput[recordIndex];
        auto reference = (floatValue * doubleValue) + 2;
        auto const mv = fourthFieldsOutput[recordIndex];
        EXPECT_EQ(reference, mv);
    }
}

/**
 * @brief This test generates a map predicate, which manipulates the input buffer content
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationTwoMapPredicateTest) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    auto mappedValue = AttributeField::create("mappedValue", DataTypeFactory::createDouble());

    /* generate code for writing result tuples to output buffer */
    auto outputSchema = Schema::create()
                            ->addField("id", DataTypeFactory::createInt32())
                            ->addField("valueSmall", DataTypeFactory::createInt16())
                            ->addField("valueFloat", DataTypeFactory::createFloat())
                            ->addField("valueDouble", DataTypeFactory::createDouble())
                            ->addField(mappedValue)
                            ->addField("valueChar", DataTypeFactory::createChar())
                            ->addField("text", DataTypeFactory::createFixedChar(12));

    codeGenerator->generateCodeForScan(inputSchema, outputSchema, context);

    //predicate definition
    codeGenerator->generateCodeForMap(
        mappedValue,
        createPredicate((QueryCompilation::PredicateItem(inputSchema->get(2)) * inputSchema->get(3)) + 2),
        context);
    codeGenerator->generateCodeForMap(
        mappedValue,
        createPredicate((QueryCompilation::PredicateItem(outputSchema->get(4)) * inputSchema->get(3))),
        context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(outputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};

    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(),
                                                                       std::vector<Runtime::Execution::OperatorHandlerPtr>());

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];

    auto inputLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, nodeEngine->getBufferManager()->getBufferSize());
    auto outputLayout = Runtime::MemoryLayouts::RowLayout::create(outputSchema, nodeEngine->getBufferManager()->getBufferSize());

    auto bindedInputLayout = inputLayout->bind(inputBuffer);
    auto bindedOutputLayout = outputLayout->bind(resultBuffer);

    auto floatValueFields = Runtime::MemoryLayouts::RowLayoutField<float, true>::create(2, inputLayout, inputBuffer);
    auto doubleValueFields = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(3, inputLayout, inputBuffer);

    auto mappedValueFields = Runtime::MemoryLayouts::RowLayoutField<double, true>::create(4, outputLayout, resultBuffer);

    for (uint64_t recordIndex = 0; recordIndex < resultBuffer.getNumberOfTuples() - 1; recordIndex++) {
        auto floatValue = floatValueFields[recordIndex];
        auto doubleValue = doubleValueFields[recordIndex];
        auto reference = ((floatValue * doubleValue) + 2) * doubleValue;
        EXPECT_EQ(reference, mappedValueFields[recordIndex]);
    }
}

/**
 * @brief This test generates a window slicer
 */
TEST_F(OperatorCodeGenerationTest, DISABLED_codeGenerations) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto pipelineContext1 = QueryCompilation::PipelineContext::create();
    pipelineContext1->pipelineName = "1";
    auto input_schema = source->getSchema();

    WindowTriggerPolicyPtr triggerPolicy = OnTimeTriggerPolicyDescription::create(1000);
    auto triggerAction = Join::LazyNestLoopJoinTriggerActionDescriptor::create();
    auto distrType = DistributionCharacteristic::createCompleteWindowType();
    auto joinType = Join::LogicalJoinDefinition::JoinType::INNER_JOIN;
    Join::LogicalJoinDefinitionPtr joinDef = Join::LogicalJoinDefinition::create(
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "window$key")->as<FieldAccessExpressionNode>(),
        FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "window$key")->as<FieldAccessExpressionNode>(),
        TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Milliseconds(10)),
        distrType,
        triggerPolicy,
        triggerAction,
        1,
        1,
        joinType);

    joinDef->updateSourceTypes(input_schema, input_schema);

    auto outputSchema = Schema::create()
                            ->addField(createField("window$start", UINT64))
                            ->addField(createField("window$end", UINT64))
                            ->addField(AttributeField::create("window$key", joinDef->getLeftJoinKey()->getStamp()));
    for (const auto& field : input_schema->fields) {
        outputSchema = outputSchema->addField(field->getName(), field->getDataType());
    }
    for (const auto& field : input_schema->fields) {
        outputSchema = outputSchema->addField(field->getName(), field->getDataType());
    }
    joinDef->updateOutputDefinition(outputSchema);
    auto joinOperatorHandler = Join::JoinOperatorHandler::create(joinDef, source->getSchema());

    pipelineContext1->registerOperatorHandler(joinOperatorHandler);
    pipelineContext1->arity = QueryCompilation::PipelineContext::BinaryLeft;
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), pipelineContext1);
    auto index = codeGenerator->generateJoinSetup(joinDef, pipelineContext1, 1);
    codeGenerator->generateCodeForJoin(joinDef, pipelineContext1, index);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, pipelineContext1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    context2->pipelineName = "2";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), joinOperatorHandler);
    auto context3 = QueryCompilation::PipelineContext::create();
    context3->registerOperatorHandler(joinOperatorHandler);
    context3->arity = QueryCompilation::PipelineContext::BinaryRight;
    context3->pipelineName = "3";
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context3);
    codeGenerator->generateJoinSetup(joinDef, context3, 1);
    codeGenerator->generateCodeForJoin(joinDef, context3, 0);
    auto stage3 = codeGenerator->compile(jitCompiler, context3, QueryCompilation::QueryCompilerOptions::DEBUG);
    stage3->setup(*executionContext);

    /* prepare input tuple buffer */
    source->open();
    auto inputBuffer = source->receiveData().value();
    NES_INFO("Processing " << inputBuffer.getNumberOfTuples() << " tuples: ");

    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(input_schema, inputBuffer.getBufferSize());
    auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, inputBuffer);
    NES_DEBUG("buffer content=" << dynamicTupleBuffer);

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    stage1->setup(*executionContext);
    stage1->start(*executionContext);
    executionContext->getOperatorHandlers()[0]->start(executionContext, nodeEngine->getStateManager(), 0);
    executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
        ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
        ->start(nodeEngine->getStateManager(), 0);
    stage1->execute(inputBuffer, *executionContext, wctx);
    stage3->execute(inputBuffer, *executionContext, wctx);

    auto* stateVarLeft = executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
                             ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
                             ->getLeftJoinState();
    auto* stateVarRight = executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
                              ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
                              ->getRightJoinState();
    std::vector<int64_t> results;
    for (auto& [key, val] : stateVarLeft->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        auto lock = std::unique_lock(val->mutex());
        for (auto& list : val->getAppendList()) {
            for (auto& value : list) {
                results.push_back(value);
            }
        }
    }
    for (auto& [key, val] : stateVarRight->rangeAll()) {
        NES_DEBUG("Key: " << key << " Value: " << val);
        auto lock = std::unique_lock(val->mutex());
        for (auto& list : val->getAppendList()) {
            for (auto& value : list) {
                results.push_back(value);
            }
        }
    }
    EXPECT_EQ(results.size(), 20U);
    executionContext->getOperatorHandler<Join::JoinOperatorHandler>(0)
        ->getJoinHandler<Join::JoinHandler, int64_t, int64_t, int64_t>()
        ->stop();
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, DISABLED_codeGenerationCompleteWindowIngestionTime) {

    try {
        /* prepare objects for test */
        auto defaultSourceType = DefaultSourceType::create();
        auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->dataPort.setValue(*dataPort);
        workerConfiguration->physicalSources.add(physicalSource);
        auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

        Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
        auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
        auto codeGenerator = QueryCompilation::CCodeGenerator::create();
        auto context1 = QueryCompilation::PipelineContext::create();
        context1->pipelineName = "1";
        auto input_schema = source->getSchema();

        codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
        WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

        auto sum = SumAggregationDescriptor::on(Attribute("window$value", BasicType::UINT64));
        auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
        auto windowDefinition = LogicalWindowDefinition::create(
            {Attribute("window$key", BasicType::UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
            {sum},
            TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)),
            DistributionCharacteristic::createCompleteWindowType(),
            trigger,
            triggerAction,
            0);
        auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
        codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

        /* compile code to pipeline stage */
        auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

        auto context2 = QueryCompilation::PipelineContext::create();
        context2->pipelineName = "1";
        codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
        auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

        auto windowOutputSchema = Schema::create()
                                      ->addField(createField("_$start", UINT64))
                                      ->addField(createField("_$end", UINT64))
                                      ->addField("window$key", UINT64)
                                      ->addField("window$value", UINT64);
        auto windowHandler =
            createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
                windowDefinition,
                windowOutputSchema);
        auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

        auto executionContext =
            std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);

        auto nextPipeline = Runtime::Execution::ExecutablePipeline::create(2,
                                                                           0,
                                                                           0,
                                                                           nodeEngine->getQueryManager(),
                                                                           executionContext,
                                                                           stage2,
                                                                           1,
                                                                           {});

        auto firstPipeline = Runtime::Execution::ExecutablePipeline::create(1,
                                                                            0,
                                                                            0,
                                                                            nodeEngine->getQueryManager(),
                                                                            executionContext,
                                                                            stage1,
                                                                            1,
                                                                            {nextPipeline});

        ASSERT_TRUE(firstPipeline->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager()));
        ASSERT_TRUE(firstPipeline->start(nodeEngine->getStateManager()));
        ASSERT_TRUE(nextPipeline->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager()));
        ASSERT_TRUE(nextPipeline->start(nodeEngine->getStateManager()));
        stage1->setup(*executionContext);
        stage1->start(*executionContext);
        stage2->setup(*executionContext);
        stage2->start(*executionContext);
        windowHandler->start(nodeEngine->getStateManager(), 0);
        windowHandler->setup(executionContext);
        source->open();
        /* prepare input tuple buffer */
        auto inputBuffer = source->receiveData().value();

        /* execute Stage */
        stage1->execute(inputBuffer, *executionContext, wctx);

        //check partial aggregates in window state
        auto* stateVar = windowHandler->getTypedWindowState();
        auto keyZero = stateVar->get(0);
        EXPECT_EQ(keyZero.value()->getPartialAggregates()[0], 5UL);
        EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5UL);

    } catch (std::exception& e) {
        NES_ERROR(e.what());
        ASSERT_TRUE(false);
    }
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, DISABLED_codeGenerationCompleteWindowEventTime) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();

    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("window$value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = LogicalWindowDefinition::create(
        {Attribute("window$key", BasicType::UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
        {sum},
        TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("window$value")), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(),
        trigger,
        triggerAction,
        0);
    auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
    codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField("window$key", UINT64)
                                  ->addField("window$value", UINT64);
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition,
            windowOutputSchema);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);

    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);

    auto nextPipeline =
        Runtime::Execution::ExecutablePipeline::create(2, 0, 0, nodeEngine->getQueryManager(), executionContext, stage2, 1, {});
    auto firstPipeline = Runtime::Execution::ExecutablePipeline::create(1,
                                                                        0,
                                                                        0,
                                                                        nodeEngine->getQueryManager(),
                                                                        executionContext,
                                                                        stage1,
                                                                        1,
                                                                        {nextPipeline});

    ASSERT_TRUE(firstPipeline->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager()));
    ASSERT_TRUE(firstPipeline->start(nodeEngine->getStateManager()));
    ASSERT_TRUE(nextPipeline->setup(nodeEngine->getQueryManager(), nodeEngine->getBufferManager()));
    ASSERT_TRUE(nextPipeline->start(nodeEngine->getStateManager()));
    stage1->setup(*executionContext);
    stage1->start(*executionContext);
    stage2->setup(*executionContext);
    stage2->start(*executionContext);
    windowHandler->start(nodeEngine->getStateManager(), 0);

    windowHandler->setup(executionContext);

    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    stage1->execute(inputBuffer, *executionContext, wctx);

    //check partial aggregates in window state
    auto* stateVar = windowHandler->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5UL);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5UL);
}

/**
 * @brief This test generates a window assigner
 */
TEST_F(OperatorCodeGenerationTest, DISABLED_codeGenerationCompleteWindowEventTimeWithTimeUnit) {
    /* prepare objects for test */
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    auto source = createWindowTestDataSource(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context1 = QueryCompilation::PipelineContext::create();

    auto input_schema = source->getSchema();

    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context1);
    WindowTriggerPolicyPtr trigger = OnTimeTriggerPolicyDescription::create(1000);

    auto sum = SumAggregationDescriptor::on(Attribute("window$value", BasicType::UINT64));
    auto triggerAction = Windowing::CompleteAggregationTriggerActionDescriptor::create();
    auto windowDefinition = LogicalWindowDefinition::create(
        {Attribute("window$key", BasicType::UINT64).getExpressionNode()->as<FieldAccessExpressionNode>()},
        {sum},
        TumblingWindow::of(TimeCharacteristic::createEventTime(Attribute("window$value"), Seconds()), Seconds(10)),
        DistributionCharacteristic::createCompleteWindowType(),
        trigger,
        triggerAction,
        0);
    auto aggregate = QueryCompilation::GeneratableOperators::GeneratableSumAggregation::create(sum);
    codeGenerator->generateCodeForCompleteWindow(windowDefinition, aggregate, context1, 0);

    /* compile code to pipeline stage */
    auto stage1 = codeGenerator->compile(jitCompiler, context1, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto context2 = QueryCompilation::PipelineContext::create();
    codeGenerator->generateCodeForScan(source->getSchema(), source->getSchema(), context2);
    auto stage2 = codeGenerator->compile(jitCompiler, context2, QueryCompilation::QueryCompilerOptions::DEBUG);

    auto windowOutputSchema = Schema::create()
                                  ->addField(createField("_$start", UINT64))
                                  ->addField(createField("_$end", UINT64))
                                  ->addField("window$key", UINT64)
                                  ->addField("window$value", UINT64);

    // init window handler
    auto windowHandler =
        createWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t, Windowing::ExecutableSumAggregation<uint64_t>>(
            windowDefinition,
            windowOutputSchema);
    windowHandler->start(nodeEngine->getStateManager(), 0);
    auto windowOperatorHandler = WindowOperatorHandler::create(windowDefinition, windowOutputSchema, windowHandler);
    auto executionContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);

    windowHandler->setup(executionContext);
    /* prepare input tuple buffer */
    auto inputBuffer = source->receiveData().value();

    /* execute Stage */
    auto queryContext = std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), windowOperatorHandler);
    stage1->setup(*queryContext);
    stage1->start(*queryContext);
    stage1->execute(inputBuffer, *queryContext, wctx);

    //check partial aggregates in window state
    auto* stateVar =
        windowHandler->as<Windowing::AggregationWindowHandler<uint64_t, uint64_t, uint64_t, uint64_t>>()->getTypedWindowState();
    EXPECT_EQ(stateVar->get(0).value()->getPartialAggregates()[0], 5UL);
    EXPECT_EQ(stateVar->get(1).value()->getPartialAggregates()[0], 5UL);
}

/**
 * @brief This test checks the correctness of the current CEPiteration OP
 */
TEST_F(OperatorCodeGenerationTest, codeGenerationCEPIterationOPinitialTest) {
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default", "defaultPhysical", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);
    auto nodeEngine = Runtime::NodeEngineBuilder::create(workerConfiguration).setQueryStatusListener(listener).build();

    Runtime::WorkerContext wctx{0, nodeEngine->getBufferManager(), 64};
    /* prepare objects for test */
    auto source = createTestSourceCodeGenPredicate(nodeEngine->getBufferManager(), nodeEngine->getQueryManager());
    auto codeGenerator = QueryCompilation::CCodeGenerator::create();
    auto context = QueryCompilation::PipelineContext::create();
    context->pipelineName = "1";
    auto inputSchema = source->getSchema();
    codeGenerator->generateCodeForScan(inputSchema, inputSchema, context);

    //define number of iterations
    codeGenerator->generateCodeForCEPIterationOperator(50, 150, context);

    /* generate code for writing result tuples to output buffer */
    codeGenerator->generateCodeForEmit(inputSchema, QueryCompilation::NO_OPTIMIZATION, QueryCompilation::FIELD_COPY, context);

    /* compile code to pipeline stage */
    auto stage = codeGenerator->compile(jitCompiler, context, QueryCompilation::QueryCompilerOptions::DEBUG);

    // init handler
    auto cepOperatorHandler = CEP::CEPOperatorHandler::create();
    context->registerOperatorHandler(cepOperatorHandler);

    /* prepare input tuple buffer */
    source->open();
    auto optVal = source->receiveData();
    ASSERT_TRUE(optVal.has_value());
    auto inputBuffer = *optVal;

    /* execute Stage */
    auto queryContext =
        std::make_shared<TestPipelineExecutionContext>(nodeEngine->getQueryManager(), context->getOperatorHandlers());

    cepOperatorHandler->start(queryContext, nodeEngine->getStateManager(), 0);

    stage->setup(*queryContext);
    stage->start(*queryContext);
    stage->execute(inputBuffer, *queryContext, wctx);

    auto resultBuffer = queryContext->buffers[0];

    /* check for correctness, as minIteration = 50, we expect 2 outputs from the 131 tuples in the inputBuffer*/
    EXPECT_EQ(int(resultBuffer.getNumberOfTuples()), 2);

    auto rowLayout = Runtime::MemoryLayouts::RowLayout::create(inputSchema, resultBuffer.getBufferSize());
    auto dynamicTupleBuffer = Runtime::MemoryLayouts::DynamicTupleBuffer(rowLayout, resultBuffer);
    NES_DEBUG(dynamicTupleBuffer);
}
}// namespace NES

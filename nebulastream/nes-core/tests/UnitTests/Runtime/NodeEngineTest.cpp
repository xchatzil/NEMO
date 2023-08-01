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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <NesBaseTest.hpp>
#include <Network/ExchangeProtocol.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <csignal>
#include <future>
#include <gtest/gtest.h>
#include <iostream>
#include <utility>

using namespace std;
using namespace NES::Windowing;
using namespace NES::Runtime;
using namespace NES::Runtime::Execution;

#define DEBUG_OUTPUT
namespace NES {

uint64_t testQueryId = 123;

std::string expectedOutput = "+----------------------------------------------------+\n"
                             "|sum:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|10|\n"
                             "+----------------------------------------------------+";

std::string joinedExpectedOutput =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------+";

std::string joinedExpectedOutput10 =
    "+----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|10|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|20|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|30|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|40|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|50|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|60|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|70|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|80|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|90|\n"
    "+----------------------------------------------------++----------------------------------------------------+\n"
    "|sum:UINT32|\n"
    "+----------------------------------------------------+\n"
    "|100|\n"
    "+----------------------------------------------------+";

template<typename MockedNodeEngine>
std::shared_ptr<MockedNodeEngine>
createMockedEngine(const std::string& hostname, uint16_t port, uint64_t bufferSize = 8192, uint64_t numBuffers = 1024) {
    try {

        class DummyQueryListener : public AbstractQueryStatusListener {
          public:
            virtual ~DummyQueryListener() {}

            bool canTriggerEndOfStream(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifySourceTermination(QueryId, QuerySubPlanId, OperatorId, Runtime::QueryTerminationType) override {
                return true;
            }
            bool notifyQueryFailure(QueryId, QuerySubPlanId, std::string) override { return true; }
            bool notifyQueryStatusChange(QueryId, QuerySubPlanId, Runtime::Execution::ExecutableQueryPlanStatus) override {
                return true;
            }
            bool notifyEpochTermination(uint64_t, uint64_t) override { return false; }
        };

        DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
        PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
        std::vector<PhysicalSourcePtr> physicalSources{physicalSource};

        auto partitionManager = std::make_shared<Network::PartitionManager>();
        std::vector<BufferManagerPtr> bufferManager = {std::make_shared<Runtime::BufferManager>(bufferSize, numBuffers)};
        auto stateManager = std::make_shared<Runtime::StateManager>(0);
        auto queryManager = std::make_shared<Runtime::DynamicQueryManager>(std::make_shared<DummyQueryListener>(),
                                                                           bufferManager,
                                                                           0,
                                                                           1,
                                                                           nullptr,
                                                                           stateManager,
                                                                           100);
        auto networkManagerCreator = [=](const Runtime::NodeEnginePtr& engine) {
            return Network::NetworkManager::create(0,
                                                   hostname,
                                                   port,
                                                   Network::ExchangeProtocol(partitionManager, engine),
                                                   bufferManager[0]);
        };
        auto compilerOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryCompiler = QueryCompilation::DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);

        auto mockEngine = std::make_shared<MockedNodeEngine>(std::move(physicalSources),
                                                             std::make_shared<HardwareManager>(),
                                                             std::move(bufferManager),
                                                             std::move(queryManager),
                                                             std::move(networkManagerCreator),
                                                             std::move(partitionManager),
                                                             std::move(queryCompiler),
                                                             0,
                                                             1024,
                                                             12,
                                                             12);
        NES::Exceptions::installGlobalErrorListener(mockEngine);
        return mockEngine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}

class TextExecutablePipeline : public ExecutablePipelineStage {
  public:
    virtual ~TextExecutablePipeline() = default;
    std::atomic<uint64_t> count = 0;
    std::atomic<uint64_t> sum = 0;
    std::promise<bool> completedPromise;

    ExecutionResult
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& wctx) override {
        auto* tuples = inputTupleBuffer.getBuffer<uint64_t>();

        NES_INFO("Test: Start execution");

        uint64_t psum = 0;
        for (uint64_t i = 0; i < inputTupleBuffer.getNumberOfTuples(); ++i) {
            psum += tuples[i];
        }
        count += inputTupleBuffer.getNumberOfTuples();
        sum += psum;

        NES_INFO("Test: query result = Processed Block:" << inputTupleBuffer.getNumberOfTuples() << " count: " << count
                                                         << " psum: " << psum << " sum: " << sum);

        if (sum == 10) {
            NES_DEBUG("TEST: result correct");

            //TupleBuffer outputBuffer = pipelineExecutionContext.allocateTupleBuffer(); WAS THIS CODE
            TupleBuffer outputBuffer = wctx.allocateTupleBuffer();

            NES_DEBUG("TEST: got buffer");
            auto* arr = outputBuffer.getBuffer<uint32_t>();
            arr[0] = static_cast<uint32_t>(sum.load());
            outputBuffer.setNumberOfTuples(1);
            NES_DEBUG("TEST: " << this << " written " << arr[0]);
            pipelineExecutionContext.emitBuffer(outputBuffer, wctx);
            completedPromise.set_value(true);
        } else {
            NES_DEBUG("TEST: result wrong ");
            completedPromise.set_value(false);
        }

        NES_DEBUG("TEST: return");
        return ExecutionResult::Ok;
    }
};

/**
 * @brief test for the engine
 * TODO: add more test cases
 *  - More complex queryIdAndCatalogEntryMapping
 *  - concurrent queryIdAndCatalogEntryMapping
 *  - long running queryIdAndCatalogEntryMapping
 *
 */
class NodeEngineTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("EngineTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup EngineTest test class.");
    }

    void SetUp() override {
        NES_DEBUG("Setup OperatorOperatorCodeGenerationTest test case.");
        Testing::NESBaseTest::SetUp();
        dataPort = Testing::NESBaseTest::getAvailablePort();
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
};
/**
 * Helper Methods
 */
void testOutput(const std::string& path) {
    ifstream testFile(path.c_str());
    ASSERT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    ASSERT_TRUE(response == 0);
}

void testOutput(const std::string& path, const std::string& expectedOutput) {
    ifstream testFile(path.c_str());
    ASSERT_TRUE(testFile.good());
    std::ifstream ifs(path.c_str());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    EXPECT_EQ(content, expectedOutput);
    ifs.close();
    int response = remove(path.c_str());
    ASSERT_TRUE(response == 0);
}

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, const DataSinkPtr& sink)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            queryManager->getBufferManager(),
            queryManager->getNumberOfWorkerThreads(),
            [sink](TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
                sink->writeData(buffer, worker);
            },
            [sink](TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>{}){
            // nop
        };
};

auto setupQEP(const NodeEnginePtr& engine, QueryId queryId, const std::string& outPath) {
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);

    DataSinkPtr sink = createTextFileSink(sch, queryId, queryId, engine, 1, outPath, false);
    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<TextExecutablePipeline>();
    auto pipeline = ExecutablePipeline::create(0, 0, queryId, engine->getQueryManager(), context, executable, 1, {sink});
    auto source =
        createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 0, 12, {pipeline});
    auto executionPlan = ExecutableQueryPlan::create(queryId,
                                                     queryId,
                                                     {source},
                                                     {sink},
                                                     {pipeline},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    return std::make_tuple(executionPlan, executable);
}

//TODO: add test for register and start only

/**
 * Test methods
 *     cout << "Stats=" << ptr->getStatistics() << endl;
 */
TEST_F(NodeEngineTest, testStartStopEngineEmpty) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, teststartDeployStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(engine->stopQuery(qep->getQueryId()));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testStartDeployUndeployStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    while (engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(engine->undeployQuery(testQueryId));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testStartRegisterStartStopDeregisterStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->registerQueryInNodeEngine(qep));
    ASSERT_TRUE(engine->startQuery(testQueryId));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    pipeline->completedPromise.get_future().get();
    ASSERT_TRUE(engine->stopQuery(testQueryId));

    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Stopped);

    ASSERT_TRUE(engine->unregisterQuery(testQueryId));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "test.out");
}
//
TEST_F(NodeEngineTest, testParallelDifferentSource) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    //  GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createTextFileSink(sch1, 1, 1, engine, 1, getTestResourceFolder() / "qep1.txt", false);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, 1, engine->getQueryManager(), context1, executable1, 1, {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                1,
                                                                0,
                                                                12,
                                                                {pipeline1});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink2 = createTextFileSink(sch2, 2, 2, engine, 1, getTestResourceFolder() / "qep2.txt", false);
    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(0, 2, 2, engine->getQueryManager(), context2, executable2, 1, {sink2});
    auto source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                2,
                                                                0,
                                                                12,
                                                                {pipeline2});
    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source2}, {sink2}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    ASSERT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->stopQuery(1, Runtime::QueryTerminationType::HardStop));
    ASSERT_TRUE(engine->stopQuery(2, Runtime::QueryTerminationType::HardStop));

    ASSERT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Stopped);
    ASSERT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Stopped);

    ASSERT_TRUE(engine->stop());

    ASSERT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Invalid);
    ASSERT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Invalid);

    testOutput(getTestResourceFolder() / "qep1.txt");
    testOutput(getTestResourceFolder() / "qep2.txt");
}
//
TEST_F(NodeEngineTest, testParallelSameSource) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);

    auto sink1 = createTextFileSink(sch1, 1, 1, engine, 1, getTestResourceFolder() / "qep1.txt", true);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, 1, engine->getQueryManager(), context1, executable1, 1, {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                1,
                                                                0,
                                                                12,
                                                                {pipeline1});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink2 = createTextFileSink(sch2, 2, 2, engine, 1, getTestResourceFolder() / "qep2.txt", true);

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink2);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(1, 2, 2, engine->getQueryManager(), context2, executable2, 1, {sink2});
    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                         engine->getQueryManager(),
                                                                         2,
                                                                         1,
                                                                         12,
                                                                         {pipeline2});
    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source2}, {sink2}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    while (engine->getQueryStatus(2) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    while (engine->getQueryStatus(1) != Runtime::Execution::ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "qep1.txt");
    testOutput(getTestResourceFolder() / "qep2.txt");
}
//
TEST_F(NodeEngineTest, DISABLED_testParallelSameSink) {// shared sinks are not supported
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    // create two executable query plans, which emit to the same sink
    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sharedSink = createTextFileSink(sch1, 0, 0, engine, 1, getTestResourceFolder() / "qep12.txt", false);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(1, 1, 1, engine->getQueryManager(), context1, executable1, 1, {sharedSink});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                1,
                                                                0,
                                                                12,
                                                                {pipeline1});
    auto executionPlan = ExecutableQueryPlan::create(1,
                                                     1,
                                                     {source1},
                                                     {sharedSink},
                                                     {pipeline1},
                                                     engine->getQueryManager(),
                                                     engine->getBufferManager());

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sharedSink);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(2, 2, 2, engine->getQueryManager(), context2, executable2, 1, {sharedSink});

    DataSourcePtr source2 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                         engine->getQueryManager(),
                                                                         2,
                                                                         3,
                                                                         12,
                                                                         {pipeline2});

    auto executionPlan2 = ExecutableQueryPlan::create(2,
                                                      2,
                                                      {source2},
                                                      {sharedSink},
                                                      {pipeline2},
                                                      engine->getQueryManager(),
                                                      engine->getBufferManager());

    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    ASSERT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());
    testOutput(getTestResourceFolder() / "qep12.txt", joinedExpectedOutput);
}
//
TEST_F(NodeEngineTest, DISABLED_testParallelSameSourceAndSinkRegstart) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    SchemaPtr sch1 = Schema::create()->addField("sum", BasicType::UINT32);
    auto sink1 = createTextFileSink(sch1, 0, 0, engine, 1, getTestResourceFolder() / "qep3.txt", true);
    auto context1 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    auto pipeline1 = ExecutablePipeline::create(0, 1, 1, engine->getQueryManager(), context1, executable1, 1, {sink1});

    auto context2 = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
    auto pipeline2 = ExecutablePipeline::create(1, 2, 2, engine->getQueryManager(), context2, executable2, 1, {sink1});
    auto source1 = createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(),
                                                                engine->getQueryManager(),
                                                                1,
                                                                4,
                                                                12,
                                                                {pipeline1, pipeline2});
    auto executionPlan =
        ExecutableQueryPlan::create(1, 1, {source1}, {sink1}, {pipeline1}, engine->getQueryManager(), engine->getBufferManager());

    auto executionPlan2 =
        ExecutableQueryPlan::create(2, 2, {source1}, {sink1}, {pipeline2}, engine->getQueryManager(), engine->getBufferManager());

    //GeneratedQueryExecutionPlanBuilder builder1 = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source1 =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    /*
    DataSinkPtr sink1 = createTextFileSink(sch1, 0, engine, 1, getTestResourceFolder() / "qep3.txt", true);
    //builder1.addSource(source1);
    builder1.addSink(sink1);
    builder1.setQueryId(1);
    builder1.setQuerySubPlanId(1);
    builder1.setQueryManager(engine->getQueryManager());
    builder1.setBufferManager(engine->getBufferManager());
    //builder1.setCompiler(engine->getCompiler());
    auto context1 =
        std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable1 = std::make_shared<TextExecutablePipeline>();
    //auto pipeline1 = ExecutablePipeline::create(0, 1, executable1, context1, 1, nullptr, source1->getSchema(), sch1);
    //builder1.addPipeline(pipeline1);

    GeneratedQueryExecutionPlanBuilder builder2 = GeneratedQueryExecutionPlanBuilder::create();

    SchemaPtr sch2 = Schema::create()->addField("sum", BasicType::UINT32);
    //builder2.addSource(source1);
    builder2.addSink(sink1);
    builder2.setQueryId(2);
    builder2.setQuerySubPlanId(2);
    builder2.setQueryManager(engine->getQueryManager());
    builder2.setBufferManager(engine->getBufferManager());
    //builder2.setCompiler(engine->getCompiler());

    auto context2 =
        std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink1);
    auto executable2 = std::make_shared<TextExecutablePipeline>();
        */
    // auto pipeline2 = ExecutablePipeline::create(0, 2, executable2, context2, 1, nullptr, source1->getSchema(), sch2);
    // builder2.addPipeline(pipeline2);

    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(engine->registerQueryInNodeEngine(executionPlan2));

    ASSERT_TRUE(engine->startQuery(1));
    ASSERT_TRUE(engine->startQuery(2));

    executable1->completedPromise.get_future().get();
    executable2->completedPromise.get_future().get();

    ASSERT_TRUE(engine->getQueryStatus(1) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->getQueryStatus(2) == ExecutableQueryPlanStatus::Running);

    ASSERT_TRUE(engine->undeployQuery(1));
    ASSERT_TRUE(engine->undeployQuery(2));
    ASSERT_TRUE(engine->stop());

    testOutput(getTestResourceFolder() / "qep3.txt", joinedExpectedOutput);
}
//
TEST_F(NodeEngineTest, testStartStopStartStop) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();

    auto [qep, pipeline] = setupQEP(engine, testQueryId, getTestResourceFolder() / "test.out");
    ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    pipeline->completedPromise.get_future().get();
    while (engine->getQueryStatus(testQueryId) != ExecutableQueryPlanStatus::Finished) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(engine->undeployQuery(testQueryId));
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);

    ASSERT_TRUE(engine->stop());
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Invalid);
    testOutput(getTestResourceFolder() / "test.out");
}

TEST_F(NodeEngineTest, testBufferData) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_FALSE(engine->bufferData(0, 0));
}

TEST_F(NodeEngineTest, testReconfigureSink) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_FALSE(engine->updateNetworkSink(0, "test", 0, 0, 0));
}

namespace detail {
void segkiller() { raise(SIGSEGV); }

void assertKiller() {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        using Runtime::NodeEngine::NodeEngine;

        explicit MockedNodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(nodeEngineId),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<NES::Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string callstack) override {
            ASSERT_TRUE(stop(false));
            ASSERT_TRUE(
                strcmp(exception->what(), "Failed assertion on false error message: this will fail now with a RuntimeException")
                == 0);
            NodeEngine::onFatalException(exception, std::move(callstack));
        }
    };
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 31340);
    NES_ASSERT(false, "this will fail now with a RuntimeException");
}
}// namespace detail

TEST_F(NodeEngineTest, DISABLED_testExceptionCrash) { EXPECT_EXIT(detail::assertKiller(), testing::ExitedWithCode(1), ""); }

TEST_F(NodeEngineTest, DISABLED_testSemiUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;
        explicit MockedNodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(nodeEngineId),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<NES::Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR(str);
            ASSERT_TRUE(strcmp(str, "Got fatal error on thread 0: Catch me if you can!") == 0);
            completedPromise.set_value(true);
            ASSERT_TRUE(stop(true));
        }
    };
    class FailingTextExecutablePipeline : public ExecutablePipelineStage {
      public:
        virtual ~FailingTextExecutablePipeline() = default;
        ExecutionResult execute(TupleBuffer&, PipelineExecutionContext&, WorkerContext&) override {
            NES_DEBUG("Going to throw exception");
            throw std::runtime_error("Catch me if you can!");// :P
        }
    };

    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 0);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createTextFileSink(sch, 0, 0, engine, 1, getTestResourceFolder() / "test.out", true);
    // builder.addSource(source);
    // builder.addSink(sink);
    // builder.setQueryId(testQueryId);
    // builder.setQuerySubPlanId(testQueryId);
    // builder.setQueryManager(engine->getQueryManager());
    // builder.setBufferManager(engine->getBufferManager());
    //builder.setCompiler(engine->getCompiler());

    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<FailingTextExecutablePipeline>();
    //auto pipeline = ExecutablePipeline::create(0, testQueryId, executable, context, 1, nullptr, source->getSchema(), sch);
    //builder.addPipeline(pipeline);
    //auto qep = builder.build();
    //ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    ASSERT_TRUE(engine->completedPromise.get_future().get());
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::ErrorState);
    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, DISABLED_testFullyUnhandledExceptionCrash) {
    class MockedNodeEngine : public Runtime::NodeEngine {
      public:
        std::promise<bool> completedPromise;

        explicit MockedNodeEngine(std::vector<PhysicalSourcePtr> physicalSources,
                                  Runtime::HardwareManagerPtr hardwareManager,
                                  std::vector<NES::Runtime::BufferManagerPtr>&& bufferManagers,
                                  QueryManagerPtr&& queryMgr,
                                  std::function<Network::NetworkManagerPtr(std::shared_ptr<NodeEngine>)>&& netFuncInit,
                                  Network::PartitionManagerPtr&& partitionManager,
                                  QueryCompilation::QueryCompilerPtr&& compiler,
                                  uint64_t nodeEngineId,
                                  uint64_t numberOfBuffersInGlobalBufferManager,
                                  uint64_t numberOfBuffersInSourceLocalBufferPool,
                                  uint64_t numberOfBuffersPerWorker)
            : NodeEngine(std::move(physicalSources),
                         std::move(hardwareManager),
                         std::move(bufferManagers),
                         std::move(queryMgr),
                         std::move(netFuncInit),
                         std::move(partitionManager),
                         std::move(compiler),
                         std::make_shared<NES::Runtime::StateManager>(0),
                         std::weak_ptr<NesWorker>(),
                         std::make_shared<NES::Experimental::MaterializedView::MaterializedViewManager>(),
                         nodeEngineId,
                         numberOfBuffersInGlobalBufferManager,
                         numberOfBuffersInSourceLocalBufferPool,
                         numberOfBuffersPerWorker,
                         false) {}

        void onFatalException(const std::shared_ptr<std::exception> exception, std::string) override {
            const auto* str = exception->what();
            NES_ERROR(str);
            ASSERT_TRUE(strcmp(str, "Unknown exception caught") == 0);
            completedPromise.set_value(true);
        }
    };
    class FailingTextExecutablePipeline : public ExecutablePipelineStage {
      public:
        ExecutionResult execute(TupleBuffer&, PipelineExecutionContext&, WorkerContext&) override {
            NES_DEBUG("Going to throw exception");
            throw 1;
        }
    };
    auto engine = createMockedEngine<MockedNodeEngine>("127.0.0.1", 0);

    //GeneratedQueryExecutionPlanBuilder builder = GeneratedQueryExecutionPlanBuilder::create();
    //DataSourcePtr source =
    //    createDefaultSourceWithoutSchemaForOneBuffer(engine->getBufferManager(), engine->getQueryManager(), 1, 12);
    SchemaPtr sch = Schema::create()->addField("sum", BasicType::UINT32);
    DataSinkPtr sink = createTextFileSink(sch, 0, 0, engine, 1, getTestResourceFolder() / "test.out", true);
    //builder.addSource(source);
    // builder.addSink(sink);
    //builder.setQueryId(testQueryId);
    //builder.setQuerySubPlanId(testQueryId);
    //builder.setQueryManager(engine->getQueryManager());
    //builder.setBufferManager(engine->getBufferManager());
    //builder.setCompiler(engine->getCompiler());

    auto context = std::make_shared<MockedPipelineExecutionContext>(engine->getQueryManager(), sink);
    auto executable = std::make_shared<FailingTextExecutablePipeline>();
    //auto pipeline = ExecutablePipeline::create(0, testQueryId, executable, context, 1, nullptr, source->getSchema(), sch);
    //builder.addPipeline(pipeline);
    // auto qep = builder.build();
    //ASSERT_TRUE(engine->deployQueryInNodeEngine(qep));
    engine->completedPromise.get_future().get();
    ASSERT_TRUE(engine->getQueryStatus(testQueryId) == ExecutableQueryPlanStatus::Running);
    ASSERT_TRUE(engine->stop());
}

TEST_F(NodeEngineTest, DISABLED_testFatalCrash) {
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("test", "test1", defaultSourceType);
    auto workerConfiguration = WorkerConfiguration::create();
    workerConfiguration->dataPort.setValue(*dataPort);
    workerConfiguration->physicalSources.add(physicalSource);

    auto engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                      .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                      .build();
    EXPECT_EXIT(detail::segkiller(), testing::ExitedWithCode(1), "Runtime failed fatally");
}

}// namespace NES

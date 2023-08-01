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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/InvalidQueryException.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class QueryDeploymentTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }
};

/**
 * Test deploying unionWith query with source on two different worker node using bottom up strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("car", "physical_car", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create();
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("truck", "physical_truck", defaultSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query=" << query);
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "car$id:INTEGER,car$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
    remove(outputFilePath.c_str());
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);)";
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("car", "physical_car", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create();
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("truck", "physical_truck", defaultSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingTopDown.out";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    string expectedContent = "car$id:INTEGER,car$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
    remove(outputFilePath.c_str());
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutput) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test");

    for (uint32_t i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, i}, 2);// fills record store of source with id 0-9
    }

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}, {1, 5}, {1, 6}, {1, 7}, {1, 8}, {1, 9}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief Test deploy query with print sink with one worker using top down strategy
 */
TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputUsingTopDownStrategy) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test");
    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2);
    }
    testHarness.validate().setupTopology();

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerFileOutput) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test") //2
                                  .attachWorkerWithMemorySourceToCoordinator("test");//3

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({1, 1}, 3);
    }
    testHarness.validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1},
                                          {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(QueryDeploymentTest, testSourceSharing) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;

    auto schema = Schema::create()
                      ->addField(createField("id", UINT64))
                      ->addField(createField("value", UINT64))
                      ->addField(createField("timestamp", UINT64));

    auto logicalSource = LogicalSource::create("window1", schema);
    coordinatorConfig->logicalSources.add(logicalSource);

    coordinatorConfig->worker.enableSourceSharing = true;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;
    coordinatorConfig->worker.queryCompiler.outputBufferOptimizationLevel =
        QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::NO;

    std::promise<bool> start;
    bool started = false;
    auto func1 = [&start, &started](NES::Runtime::TupleBuffer& buffer, uint64_t numTuples) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        if (!started) {
            start.get_future().get();
            started = true;
        }

        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numTuples; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = 0;
        }
    };

    auto lambdaSourceType1 = LambdaSourceType::create(std::move(func1), 2, 2, GatheringMode::INTERVAL_MODE);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream1", lambdaSourceType1);
    coordinatorConfig->worker.physicalSources.add(physicalSource1);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);

    std::string outputFilePath1 = getTestResourceFolder() / "testOutput1.out";
    remove(outputFilePath1.c_str());

    std::string outputFilePath2 = getTestResourceFolder() / "testOutput2.out";
    remove(outputFilePath2.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("testSourceSharing: Submit query");

    string query1 =
        R"(Query::from("window1")
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));

    string query2 =
        R"(Query::from("window1")
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));

    start.set_value(true);

    string expectedContent1 = "window1$id:INTEGER,window1$value:INTEGER,window1$timestamp:INTEGER\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "5,5,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "5,5,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath2));

    NES_DEBUG("testSourceSharing: Remove query 1");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_DEBUG("testSourceSharing: Remove query 2");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));

    NES_DEBUG("testSourceSharing: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("testSourceSharing: Test finished");
}

TEST_F(QueryDeploymentTest, testSourceSharingWithFilter) {
    std::string outputFilePath1 = getTestResourceFolder() / "testOutput1.out";
    remove(outputFilePath1.c_str());

    std::string outputFilePath2 = getTestResourceFolder() / "testOutput2.out";
    remove(outputFilePath2.c_str());

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;

    auto schema = Schema::create()
                      ->addField(createField("id", UINT64))
                      ->addField(createField("value", UINT64))
                      ->addField(createField("timestamp", UINT64));

    auto logicalSource = LogicalSource::create("window1", schema);
    coordinatorConfig->logicalSources.add(logicalSource);

    coordinatorConfig->worker.enableSourceSharing = true;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;
    coordinatorConfig->worker.queryCompiler.outputBufferOptimizationLevel =
        QueryCompilation::QueryCompilerOptions::OutputBufferOptimizationLevel::NO;

    std::promise<bool> start;
    bool started = false;
    auto func1 = [&start, &started](NES::Runtime::TupleBuffer& buffer, uint64_t numTuples) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        if (!started) {
            start.get_future().get();
            started = true;
        }

        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numTuples; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = 0;
        }
    };

    auto lambdaSourceType1 = LambdaSourceType::create(std::move(func1), 2, 2, GatheringMode::INTERVAL_MODE);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream1", lambdaSourceType1);
    coordinatorConfig->worker.physicalSources.add(physicalSource1);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("testSourceSharing: Submit query");

    string query1 =
        R"(Query::from("window1").filter(Attribute("id") < 5)
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));

    string query2 =
        R"(Query::from("window1").filter(Attribute("id") > 5)
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));

    start.set_value(true);

    string expectedContent1 = "window1$id:INTEGER,window1$value:INTEGER,window1$timestamp:INTEGER\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n";

    string expectedContent2 = "window1$id:INTEGER,window1$value:INTEGER,window1$timestamp:INTEGER\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent2, outputFilePath2));

    NES_DEBUG("testSourceSharing: Remove query 1");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_DEBUG("testSourceSharing: Remove query 2");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));

    NES_DEBUG("testSourceSharing: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("testSourceSharing: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerFileOutputUsingTopDownStrategy) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test"))";
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", defaultLogicalSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test") //2
                           .attachWorkerWithMemorySourceToCoordinator("test");//3

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({1, 1}, 3);
    }

    testHarness.validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1},
                                          {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
#ifdef TFDEF
TEST_F(QueryDeploymentTest, testDeployTwoWorkerFileOutputWithInferModel) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test").inferModel(")" + std::string(TEST_DATA_DIRECTORY) + R"(iris_95acc.tflite",
                        {Attribute("id"), Attribute("id"), Attribute("id"), Attribute("id")},
                        {Attribute("iris0", FLOAT32), Attribute("iris1", FLOAT32), Attribute("iris2", FLOAT32)})
                        .filter(Attribute("iris0") > 0))";
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", defaultLogicalSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test") //2
                           .attachWorkerWithMemorySourceToCoordinator("test");//3

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({1, 1}, 3);
    }
    testHarness.validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);
    struct Output {
        uint32_t id;
        uint32_t value;
        float_t iris0;
        float_t iris1;
        float_t iris2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(20, "BottomUp", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), 20);
}
#endif

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilter) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test").filter(Attribute("id") < 5))";
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", defaultLogicalSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test");

    for (int i = 0; i < 5; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({5, 1}, 2);
    }

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{1, 1}, {1, 1}, {1, 1}, {1, 1}, {1, 1}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenTwoSourcesRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("id") < 5).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenOneSourceRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("id") < 5).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenNoSourceRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream").filter(Attribute("id") < 5).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithProjection) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    string query = R"(Query::from("test").project(Attribute("id")))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test");

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2);
    }

    testHarness.validate().setupTopology();

    struct Output {
        uint32_t id;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id); }
    };

    std::vector<Output> expectedOutput = {{1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}, {1}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithWrongProjection) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create();
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_car", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->workerHealthCheckWaitTime = 1;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("default_logical").project(Attribute("asd")).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    EXPECT_THROW(
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY),
        InvalidQueryException);

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesTwoWorkerFileOutput) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    auto wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    auto defaultSource1 = DefaultSourceType::create();
    auto physicalSource1 = PhysicalSource::create("default_logical", "x1", defaultSource1);
    wrkConf1->physicalSources.add(physicalSource1);
    wrkConf1->enableStatisticOuput = true;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    auto wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    auto defaultSource2 = DefaultSourceType::create();
    auto physicalSource2 = PhysicalSource::create("default_logical", "x2", defaultSource2);
    wrkConf2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    string query2 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath2
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithOutput) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream1", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    csvSourceType2->setSkipHeader(true);
    auto physicalSource2 = PhysicalSource::create("stream2", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource2);

    workerConfig1->sourcePinList = ("0,1");
    workerConfig1->queuePinList = ("0,1");
    workerConfig1->numWorkerThreads = (2);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 =
        R"(Query::from("stream1").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        R"(Query::from("stream2").sink(FileSinkDescriptor::create(")" + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    string expectedContent1 = "stream1$value:INTEGER,stream1$id:INTEGER,stream1$timestamp:INTEGER\n"
                              "1,12,1001\n";

    string expectedContent2 = "stream2$value:INTEGER,stream2$id:INTEGER,stream2$timestamp:INTEGER\n"
                              "1,12,1001\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent2, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithHardShutdownAndStatic) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream1", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(100);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(100);
    csvSourceType2->setSkipHeader(true);
    auto physicalSource2 = PhysicalSource::create("stream2", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource2);

    workerConfig1->sourcePinList = ("0,1");
    workerConfig1->queuePinList = ("0,1");
    workerConfig1->numWorkerThreads = (2);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 =
        R"(Query::from("stream1").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        R"(Query::from("stream2").sink(FileSinkDescriptor::create(")" + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));

    string expectedContent1 = "stream1$value:INTEGER,stream1$id:INTEGER,stream1$timestamp:INTEGER\n"
                              "1,12,1001\n";

    string expectedContent2 = "stream2$value:INTEGER,stream2$id:INTEGER,stream2$timestamp:INTEGER\n"
                              "1,12,1001\n";

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesOnTwoWorkerFileOutputWithQueryMerging) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create();
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_car", defaultSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create();
    auto physicalSource2 = PhysicalSource::create("default_logical", "physical_car", defaultSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    string query2 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath2
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerJoinUsingTopDownOnSameSchema) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(2);
    csvSourceType->setSkipHeader(true);

    string query =
        R"(Query::from("window").joinWith(Query::from("window2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
    Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window", testSchema)
                                  .addLogicalSource("window2", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator("window", csvSourceType)
                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t window1Value;
        uint64_t window1Id;
        uint64_t window1Timestamp;
        uint64_t window2Value;
        uint64_t window2Id;
        uint64_t window2Timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && window1Value == rhs.window1Value
                    && window1Id == rhs.window1Id && window1Timestamp == rhs.window1Timestamp && window2Value == rhs.window2Value
                    && window2Id == rhs.window2Id && window2Timestamp == rhs.window2Timestamp);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithHardShutdown) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("stream1", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(100);
    csvSourceType1->setSkipHeader(true);
    auto physicalSource1 = PhysicalSource::create("stream1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(100);
    csvSourceType2->setSkipHeader(true);
    auto physicalSource2 = PhysicalSource::create("stream2", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource2);

    workerConfig1->sourcePinList = "0,1";
    workerConfig1->numWorkerThreads = 2;
    workerConfig1->queryManagerMode = Runtime::QueryExecutionMode::Dynamic;

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 =
        R"(Query::from("stream1").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 =
        queryService->validateAndQueueAddQueryRequest(query1, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    string query2 =
        R"(Query::from("stream2").sink(FileSinkDescriptor::create(")" + outputFilePath2 + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 =
        queryService->validateAndQueueAddQueryRequest(query2, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalogService));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalogService));

    string expectedContent1 = "stream1$value:INTEGER,stream1$id:INTEGER,stream1$timestamp:INTEGER\n"
                              "1,12,1001\n";

    string expectedContent2 = "stream2$value:INTEGER,stream2$id:INTEGER,stream2$timestamp:INTEGER\n"
                              "1,12,1001\n";

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalogService, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalogService, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}
}// namespace NES
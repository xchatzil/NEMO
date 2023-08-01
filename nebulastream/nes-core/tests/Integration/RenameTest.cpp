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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class RenameTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RenameTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RenameTest test class.");
    }
};

TEST_F(RenameTest, testAttributeRenameAndProjection) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("RenameTest: Submit query");
    string query = "Query::from(\"default_logical\").project(Attribute(\"id\").as(\"NewName\")).sink(FileSinkDescriptor::"
                   "create(\""s
        + getTestResourceFolder().c_str() + "/test.out\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("RenameTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ifstream my_file(getTestResourceFolder() / "test.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(getTestResourceFolder() / "test.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|default_logical$NewName:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "+----------------------------------------------------+";
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): content=" << content);
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}

TEST_F(RenameTest, testAttributeRenameAndProjectionMapTestProjection) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    auto outputFile = getTestResourceFolder() / "test.out";

    NES_INFO("RenameTest: Submit query");
    string query = "Query::from(\"default_logical\")"
                   ".project(Attribute(\"id\").as(\"NewName\"))"
                   ".map(Attribute(\"NewName\") = Attribute(\"NewName\") * 2u)"
                   ".project(Attribute(\"NewName\").as(\"id\"))"
                   ".sink(FileSinkDescriptor::create(\""s
        + outputFile.c_str() + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("RenameTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ifstream my_file(outputFile);
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs(outputFile);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|default_logical$id:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "|2|\n"
                             "+----------------------------------------------------+";
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): content=" << content);
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}

TEST_F(RenameTest, testAttributeRenameAndFilter) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;

    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create("default_logical", "default", defaultSourceType);
    wrkConf->physicalSources.add(physicalSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    auto outputFile = getTestResourceFolder() / "test.out";

    NES_INFO("RenameTest: Submit query");
    std::string query =
        R"(Query::from("default_logical").filter(Attribute("id") < 2).project(Attribute("id").as("NewName"), Attribute("value")).sink(FileSinkDescriptor::create(")";
    query += outputFile;
    query += R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("RenameTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent = "default_logical$NewName:INTEGER,default_logical$value:INTEGER\n"
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

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFile));

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}

TEST_F(RenameTest, testCentralWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema1 =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window", testSchema1);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("RenameTest: Submit query");

    string query = "Query::from(\"window\")"
                   ".project(Attribute(\"id\").as(\"newId\"), Attribute(\"timestamp\"), Attribute(\"value\").as(\"newValue\"))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), Seconds(1)))"
                   ".byKey(Attribute(\"newId\")).apply(Sum(Attribute(\"newValue\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$newId:INTEGER,window$newValue:INTEGER\n"
                             "1000,2000,1,1\n"
                             "2000,3000,1,2\n"
                             "1000,2000,4,1\n"
                             "2000,3000,11,2\n"
                             "1000,2000,12,1\n"
                             "2000,3000,16,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("RenameTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}

/**
 * Test deploying join with different sources
 */
TEST_F(RenameTest, DISABLED_testJoinWithDifferentSourceTumblingWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical source
    std::string testSchema1 =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window1", testSchema1);
    std::string testSchema2 =
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("window2", testSchema2);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("RenameTest: Submit query");
    string query =
        R"(Query::from("window1")
            .project(Attribute("id1").as("id1New"), Attribute("timestamp"))
            .joinWith(Query::from("window2").project(Attribute("id2").as("id2New"), Attribute("timestamp")))
            .where(Attribute("id1New")).equalsTo(Attribute("id2New")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
            Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1New:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2New:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,11,2,11,2001,2,11,2301\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("RenameTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("RenameTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}
}// namespace NES

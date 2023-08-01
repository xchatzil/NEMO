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

#include <API/Query.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <NesBaseTest.hpp>
#include <REST/ServerTypes.hpp>
#include <Runtime/BufferManager.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/MetricValidator.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>

namespace NES {

class MonitoringControllerTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    uint16_t timeout = 15;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MonitoringControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MonitoringControllerTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("Setup MonitoringControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MonitoringControllerTest test class."); }
};

TEST_F(MonitoringControllerTest, testStartMonitoring) {
    NES_INFO("Tests for Oatpp Monitoring Controller start monitoring: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // oatpp GET start call
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    EXPECT_EQ(r.status_code, 200);

    //check if content of r contains valid information
    std::set<std::string> expectedMonitoringStreams{"wrapped_network", "wrapped_cpu", "memory", "disk"};
    nlohmann::json jsonsStart = nlohmann::json::parse(r.text);
    NES_INFO("MonitoringControllerTest - Received Data from GetStart request: " << jsonsStart.dump());
    ASSERT_EQ(jsonsStart.size(), expectedMonitoringStreams.size());
    bool check = MetricValidator::checkEntriesOfStream(expectedMonitoringStreams, jsonsStart);
    ASSERT_TRUE(check);
}

TEST_F(MonitoringControllerTest, testStopMonitoring) {
    NES_INFO("Tests for Oatpp Monitoring Controller start monitoring: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/stop"});
    EXPECT_EQ(r.status_code, 200);
}

TEST_F(MonitoringControllerTest, testStartMonitoringFailsBecauseMonitoringIsNotEnabled) {
    NES_INFO("Tests for Oatpp Monitoring Controller start monitoring: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    auto future = cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 500);
}

TEST_F(MonitoringControllerTest, testStopMonitoringFailsBecauseMonitoringIsNotEnabled) {
    NES_INFO("Tests for Oatpp Monitoring Controller: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    auto future = cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/stop"});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 500);
}

TEST_F(MonitoringControllerTest, testRequestAllMetrics) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testRequestAllMetrics: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }

    auto future = cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/metrics"});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 200);

    nlohmann::json jsonsOfResponse = nlohmann::json::parse(r.text);
    NES_INFO("MonitoringControllerTest - Received Data from GetAllMetrics request: " << jsonsOfResponse);

    //check if content of r contains valid information (right fields and valid queryIds):
    ASSERT_EQ(jsonsOfResponse.size(), 1);
    auto json = jsonsOfResponse[std::to_string(1)];
    NES_INFO("MonitoringControllerTest: Requesting monitoring data from node with ID " << std::to_string(1));
    NES_INFO("Received Data for node 1: " << json.dump());
    ASSERT_TRUE(MetricValidator::isValidAll(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    ASSERT_TRUE(MetricValidator::checkNodeIds(json, 1));
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerDataFromOneNode) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerDataFromOneNode: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/metrics"},
                      cpr::Parameters{{"nodeId", std::to_string(1)}});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 200);
    nlohmann::json jsonsOfResponse = nlohmann::json::parse(r.text);
    NES_INFO("MonitoringControllerTest - Received Data from GetMetricsForOneNode request: " << jsonsOfResponse);

    //check if content of r contains valid information (right fields and valid queryIds):
    ASSERT_EQ(jsonsOfResponse.size(), 1);
    auto json = jsonsOfResponse[std::to_string(1)];
    NES_INFO("MonitoringControllerTest: Requesting monitoring data from node with ID " << std::to_string(1));
    ASSERT_TRUE(MetricValidator::isValidAll(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), json));
    ASSERT_TRUE(MetricValidator::checkNodeIds(json, 1));
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerStorage) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerStorage: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    MonitoringServicePtr monitoringService = coordinator->getMonitoringService();
    auto expected = monitoringService->requestNewestMonitoringDataFromMetricStoreAsJson();
    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/storage"});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 200);
    //compare content of response to expected values
    nlohmann::json jsons = nlohmann::json::parse(r.text);
    ASSERT_EQ(jsons.size(), 1);
    NES_INFO("MonitoringControllerTest - Received Data from Get-Storage request: " << jsons);
    auto json = jsons[std::to_string(1)];
    NES_INFO("MonitoringControllerTest: Requesting monitoring data from node with ID " << std::to_string(1));
    NES_INFO("Received Data for node 1: " << json.dump());
    auto jsonRegistration = json["registration"][0]["value"];
    ASSERT_TRUE(MetricValidator::isValidRegistrationMetrics(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(),
                                                            jsonRegistration));
    ASSERT_EQ(jsonRegistration["NODE_ID"], 1);
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerStreams) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerStreams: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    MonitoringServicePtr monitoringService = coordinator->getMonitoringService();
    auto expected = monitoringService->startMonitoringStreams();
    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/streams"});
    future.wait();
    auto r = future.get();
    EXPECT_EQ(r.status_code, 200);

    //compare content of response to expected values
    nlohmann::json jsons = nlohmann::json::parse(r.text);
    NES_INFO("MonitoringControllerTest - Received Data from Get-Streams request: " << jsons);
    std::set<std::string> expectedMonitoringStreams{"wrapped_network", "wrapped_cpu", "memory", "disk"};
    bool check = MetricValidator::checkEntriesOfStream(expectedMonitoringStreams, jsons);
    ASSERT_TRUE(check);
}

}//namespace NES

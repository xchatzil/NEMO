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
#include <iostream>    // For cout
#include <netinet/in.h>// For sockaddr_in
#include <sys/socket.h>// For socket functions
#include <unistd.h>    // For read

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sources/DataSource.hpp>
#include <Sources/TCPSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>

#include <Util/TestUtils.hpp>
#include <thread>

namespace NES {

class TCPSourceIntegrationTest : public Testing::NESBaseTest {
  public:
    /**
     * @brief Set up test cases, starts a TCP server before all tests are run
     */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TCPSourceIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TCPSourceIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        NES_TRACE("TCPSourceIntegrationTest: Start TCPServer.");
        tcpServerPort = getAvailablePort();
        startServer();
    }

    void TearDown() override {
        stopServer();
        NES_TRACE("TCPSourceIntegrationTest: Stop TCPServer.");
        Testing::NESBaseTest::TearDown();
    }

    /**
     * @brief starts a TCP server on tcpServerPort
     */
    void startServer() {
        // Create a socket (IPv4, TCP)
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to create socket. errno: " << errno);
            exit(EXIT_FAILURE);
        }

        // Listen to port tcpServerPort on any address
        sockaddr.sin_family = AF_INET;
        sockaddr.sin_addr.s_addr = INADDR_ANY;
        sockaddr.sin_port = htons(*tcpServerPort);// htons is necessary to convert a number to
                                                  // network byte order
        int opt = 1;
        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to create socket. errno: " << errno);
            exit(EXIT_FAILURE);
        }
        if (bind(sockfd, (struct sockaddr*) &sockaddr, sizeof(sockaddr)) < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to bind to port " << tcpServerPort << ". errno: " << errno);
            exit(EXIT_FAILURE);
        }

        // Start listening. Hold at most 10 connections in the queue
        if (listen(sockfd, 10) < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to listen on socket. errno: " << errno);
            exit(EXIT_FAILURE);
        }
        NES_TRACE("TCPSourceIntegrationTest: TCPServer successfully started.");
    }

    /**
     * @brief stopps the TCP server running on tcpServerPort
     */
    void stopServer() {
        // Close the connections
        while (close(sockfd) < 0) {
            NES_TRACE("TCPSourceIntegrationTest: Closing Server connection pls wait ...");
        };
    }

    /**
     * @brief sends multiple messages via TCP connection. Static because this is run in a thread inside the test cases
     * @param message message as string to be send
     * @param repeatSending how of the message should be send
     */
    int sendMessages(std::string message, int repeatSending) {
        // Grab a connection from the queue
        auto addrlen = sizeof(sockaddr);

        int connection = accept(sockfd, (struct sockaddr*) &sockaddr, (socklen_t*) &addrlen);
        if (connection < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to grab connection. errno: " << errno);
            return -1;
        }

        for (int i = 0; i < repeatSending; ++i) {
            NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message << " iter=" << i);
            send(connection, message.c_str(), message.size(), 0);
        }

        // Close the connections
        return close(connection);
    }

    /**
     * @brief sending different comma seperated messages via TCP to test variable length read. Static because it is run in a
     * threat inside test cases
     */
    int sendMessageCSVVariableLength() {
        // Grab a connection from the queue
        auto addrlen = sizeof(sockaddr);

        int connection = accept(sockfd, (struct sockaddr*) &sockaddr, (socklen_t*) &addrlen);
        if (connection < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to grab connection. errno: " << errno);
            return -1;
        }

        std::string message = "100,4.986,sonne";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "192,4.96,sonne";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "130,4.9,stern";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "589,4.98621,sonne";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "39,4.198,malen";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "102,9.986,hello";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        // Close the connections
        return close(connection);
    }

    /**
     * @brief sending different JSON messages via TCP to test variable length read. Static because it is run in a
     * threat inside test cases
     */
    int sendMessageJSONVariableLength() {
        // Grab a connection from the queue
        auto addrlen = sizeof(sockaddr);

        int connection = accept(sockfd, (struct sockaddr*) &sockaddr, (socklen_t*) &addrlen);
        if (connection < 0) {
            NES_ERROR("TCPSourceIntegrationTest: Failed to grab connection. errno: " << errno);
            return -1;
        }

        std::string message = "{\"id\":\"4\", \"value\":\"5.893\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "{\"id\":\"8\", \"value\":\"5.8939\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "{\"id\":\"432\", \"value\":\"5.83\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "{\"id\":\"99\", \"value\":\"0.893\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "{\"id\":\"911\", \"value\":\"5.8893\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        message = "{\"id\":\"4293\", \"value\":\"5.89311\", \"name\":\"hello\"}";
        NES_TRACE("TCPSourceIntegrationTest: Sending message: " << message);
        send(connection, std::to_string(message.length()).c_str(), 2, 0);
        send(connection, message.c_str(), message.size(), 0);

        // Close the connections
        return close(connection);
    }

    int sockfd = 0;
    sockaddr_in sockaddr = {};
    Testing::BorrowedPortPtr tcpServerPort;
};

/**
 * @brief tests TCPSource read of csv data that is seperated by a given token. Here \n is used
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVDataWithSeparatorToken) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()->addField("id", UINT32)->addField("value", FLOAT32)->addField("onTime", BOOLEAN);

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
    sourceConfig->setTupleSeparator('\n');

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 30;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("42,5.893,true\n", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|1|\n"
        "|42|5.893000|1|\n"
        "|42|5.893000|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$onTime:BOOLEAN|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|1|\n"
        "|42|5.893000|1|\n"
        "|42|5.893000|1|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of JSON data that is seperated by a given token. Here \n is used
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadJSONDataWithSeparatorToken) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::JSON);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
    sourceConfig->setTupleSeparator('\n');

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("{\"id\":\"42\", \"value\":\"5.893\", \"name\":\"hello\"}\n", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of CSV data when obtaining the size of the data from the socket. Constant length
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVDataLengthFromSocket) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    sourceConfig->setBytesUsedForSocketBufferSizeTransfer(2);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("1442,5.893,hello", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of CSV data when obtaining the size of the data from the socket. Variable length
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVWithVariableLength) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    sourceConfig->setBytesUsedForSocketBufferSizeTransfer(2);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessageCSVVariableLength();
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|100|4.986000|sonne|\n"
        "|192|4.960000|sonne|\n"
        "|130|4.900000|stern|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|589|4.986210|sonne|\n"
        "|39|4.198000|malen|\n"
        "|102|9.986000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of JSON data when obtaining the size of the data from the socket. Constant length
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadJSONDataLengthFromSocket) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(10000);
    sourceConfig->setInputFormat(Configurations::InputFormat::JSON);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    sourceConfig->setBytesUsedForSocketBufferSizeTransfer(2);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("44{\"id\":\"42\", \"value\":\"5.893\", \"name\":\"hello\"}", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of CSV data when obtaining the size of the data from the socket. Variable length
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadJSONDataWithVariableLength) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::JSON);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET);
    sourceConfig->setBytesUsedForSocketBufferSizeTransfer(2);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessageJSONVariableLength();
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|4|5.893000|hello|\n"
        "|8|5.893900|hello|\n"
        "|432|5.830000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|99|0.893000|hello|\n"
        "|911|5.889300|hello|\n"
        "|4293|5.893110|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of CSV data with fixed length inputted at source creation time
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadCSVDataWithFixedSize) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE);
    sourceConfig->setSocketBufferSize(14);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("42,5.893,hello", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/**
 * @brief tests TCPSource read of CSV data with fixed length inputted at source creation time
 */
TEST_F(TCPSourceIntegrationTest, TCPSourceReadJSONDataWithFixedSize) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("TCPSourceIntegrationTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("TCPSourceIntegrationTest: Coordinator started successfully");

    auto tcpSchema = Schema::create()
                         ->addField("id", UINT32)
                         ->addField("value", FLOAT32)
                         ->addField("name", DataTypeFactory::createFixedChar(5));

    crd->getSourceCatalogService()->registerLogicalSource("tcpStream", tcpSchema);
    NES_DEBUG("TCPSourceIntegrationTest: Added tcpLogicalSource to coordinator.")

    NES_DEBUG("TCPSourceIntegrationTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;

    TCPSourceTypePtr sourceConfig = TCPSourceType::create();
    sourceConfig->setSocketPort(*tcpServerPort);
    sourceConfig->setSocketHost("127.0.0.1");
    sourceConfig->setSocketDomainViaString("AF_INET");
    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
    sourceConfig->setFlushIntervalMS(5000);
    sourceConfig->setInputFormat(Configurations::InputFormat::JSON);
    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE);
    sourceConfig->setSocketBufferSize(44);

    auto physicalSource = PhysicalSource::create("tcpStream", "tcpStream", sourceConfig);
    workerConfig1->physicalSources.add(physicalSource);
    workerConfig1->bufferSizeInBytes = 50;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("TCPSourceIntegrationTest: Worker1 started successfully");

    std::string filePath = getTestResourceFolder() / "tcpSourceTest.csv";
    remove(filePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    //register query
    std::string queryString = R"(Query::from("tcpStream").sink(FileSinkDescriptor::create(")" + filePath + "\"));";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(queryString, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    int connection;
    std::thread serverThread([&connection, this] {
        connection = sendMessages("{\"id\":\"42\", \"value\":\"5.893\", \"name\":\"hello\"}", 6);
    });
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));
    serverThread.join();

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    ASSERT_EQ(connection, 0);

    std::ifstream ifs(filePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|tcpStream$id:UINT32|tcpStream$value:FLOAT32|tcpStream$name:CHAR[5]|\n"
        "+----------------------------------------------------+\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "|42|5.893000|hello|\n"
        "+----------------------------------------------------+";

    NES_INFO("TCPSourceIntegrationTest: content=" << content);
    NES_INFO("TCPSourceIntegrationTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

}// namespace NES
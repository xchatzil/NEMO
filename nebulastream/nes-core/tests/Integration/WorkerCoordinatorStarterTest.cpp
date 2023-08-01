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

#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

using std::cout;
using std::endl;
#define DEBUG_OUTPUT
namespace NES {

class WorkerCoordinatorStarterTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WorkerCoordinatorStarterTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup WorkerCoordinatorStarterTest test class.");
    }
};

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;
    cout << "wakeup" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    cout << crd.use_count() << " use cnt coord" << endl;
    EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator10times) {
    cout << "start coordinator" << endl;
    for (uint64_t i = 0; i < 10; i++) {
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        cout << "iteration = " << i << endl;
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);
        EXPECT_NE(port, 0ULL);
        cout << "coordinator started successfully" << endl;

        cout << "start worker" << endl;
        workerConfig->coordinatorPort = port;
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        cout << "worker started connected successfully" << endl;

        cout << "wakeup" << endl;

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop(false);
        EXPECT_TRUE(retStopWrk);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator(false);
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        crd.reset();
        wrk.reset();
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        EXPECT_TRUE(retStopCord);
    }
}
TEST_F(WorkerCoordinatorStarterTest, startStopCoordinatorWorker) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;

    cout << "wakeup" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWithoutDisconnectWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
}

TEST_F(WorkerCoordinatorStarterTest, startConnectDisconnectStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    workerConfig->workerHealthCheckWaitTime = 1;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(WorkerCoordinatorStarterTest, startReconnectStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    workerConfig->workerHealthCheckWaitTime = 1;

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->coordinatorPort = port;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retConWrk2 = wrk->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker got connected " << endl;

    bool retDisWrk2 = wrk->disconnect();
    EXPECT_TRUE(retDisWrk2);
    cout << "worker got disconnected " << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES

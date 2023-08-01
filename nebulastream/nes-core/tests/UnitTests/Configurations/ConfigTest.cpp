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

#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/KafkaSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MQTTSourceType.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace NES {

using namespace Configurations;

class ConfigTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("Config.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup Configuration test class.");
    }

    static std::vector<const char*> makePosixArgs(const std::vector<std::string>& args) {
        static const char* empty = "";
        std::vector<const char*> argv(args.size() + 1);
        argv[0] = empty;
        std::transform(args.begin(), args.end(), argv.begin() + 1, [](const std::string& arg) {
            return arg.c_str();
        });
        return argv;
    }

    static std::map<std::string, std::string> makeCommandLineArgs(const std::vector<std::string>& args) {
        std::map<std::string, std::string> result;
        for (auto arg : args) {
            auto pos = arg.find('=');
            result.insert({arg.substr(0, pos), arg.substr(pos + 1, arg.length() - 1)});
        }
        return result;
    }

    static void TearDownTestCase() { NES_INFO("Tear down Configuration test class."); }
};

/**
 * @brief This reads an coordinator yaml and checks the configuration
 */
TEST_F(ConfigTest, testEmptyParamsAndMissingParamsCoordinatorYAMLFile) {

    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyCoordinator.yaml");
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->dataPort.getValue(), coordinatorConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfSlots.getValue(),
              coordinatorConfigPtr->worker.numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(),
              coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numWorkerThreads.getValue(),
              coordinatorConfigPtr->worker.numWorkerThreads.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(),
              coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerEpoch.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerEpoch.getDefaultValue());
}

TEST_F(ConfigTest, testLogicalSourceAndSchemaParamsCoordinatorYAMLFile) {
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    coordinatorConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY)
                                                           + "coordinatorLogicalSourceAndSchema.yaml");
    EXPECT_FALSE(coordinatorConfigPtr->logicalSources.empty());
    EXPECT_EQ(coordinatorConfigPtr->logicalSources.size(), 3);
    auto logicalSources = coordinatorConfigPtr->logicalSources.getValues();
    EXPECT_EQ(logicalSources[0].getValue()->getLogicalSourceName(), "lsn1");
    EXPECT_EQ(logicalSources[1].getValue()->getLogicalSourceName(), "lsn2");
    EXPECT_EQ(logicalSources[2].getValue()->getLogicalSourceName(), "lsn3");
    auto firstSourceSchema = logicalSources[0].getValue()->getSchema();
    auto secondSourceSchema = logicalSources[1].getValue()->getSchema();
    auto thirdSourceSchema = logicalSources[2].getValue()->getSchema();
    EXPECT_EQ(firstSourceSchema->getSize(), 3);
    EXPECT_EQ(secondSourceSchema->getSize(), 1);
    EXPECT_TRUE(firstSourceSchema->contains("csv_id"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_2"));
    EXPECT_TRUE(firstSourceSchema->contains("csv_id_3"));
    EXPECT_TRUE(secondSourceSchema->contains("csv_id_4"));
    EXPECT_TRUE(thirdSourceSchema->contains("csv_id_5"));
}

TEST_F(ConfigTest, testCoordinatorEPERATPRmptyParamsConsoleInput) {
    // given
    CoordinatorConfigurationPtr coordinatorConfigPtr = std::make_shared<CoordinatorConfiguration>();
    auto commandLineParams = makeCommandLineArgs({"--restIp=localhost",
                                                  "--worker.numberOfSlots=10",
                                                  "--worker.numberOfBuffersInSourceLocalBufferPool=128",
                                                  "--worker.bufferSizeInBytes=1024"});
    // when
    coordinatorConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
    EXPECT_EQ(coordinatorConfigPtr->restPort.getValue(), coordinatorConfigPtr->restPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->rpcPort.getValue(), coordinatorConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->dataPort.getValue(), coordinatorConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->restIp.getValue(), coordinatorConfigPtr->restIp.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->coordinatorIp.getValue(), coordinatorConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfSlots.getValue(),
              coordinatorConfigPtr->worker.numberOfSlots.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->logLevel.getValue(), coordinatorConfigPtr->logLevel.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getValue(),
              coordinatorConfigPtr->worker.numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_NE(coordinatorConfigPtr->worker.bufferSizeInBytes.getValue(),
              coordinatorConfigPtr->worker.bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->worker.numWorkerThreads.getValue(),
              coordinatorConfigPtr->worker.numWorkerThreads.getDefaultValue());

    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryBatchSize.getValue(),
              coordinatorConfigPtr->optimizer.queryBatchSize.getDefaultValue());
    EXPECT_EQ(coordinatorConfigPtr->optimizer.queryMergerRule.getValue(),
              coordinatorConfigPtr->optimizer.queryMergerRule.getDefaultValue());
}

TEST_F(ConfigTest, testEmptyParamsAndMissingParamsWorkerYAMLFile) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyWorker.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerEpoch.getValue(), workerConfigPtr->numberOfBuffersPerEpoch.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_TRUE(workerConfigPtr->physicalSources.empty());
}

TEST_F(ConfigTest, testWorkerYAMLFileWithMultiplePhysicalSource) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "workerWithPhysicalSources.yaml");

    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_TRUE(!workerConfigPtr->physicalSources.empty());
    EXPECT_TRUE(workerConfigPtr->physicalSources.size() == 2);
    for (const auto& physicalSource : workerConfigPtr->physicalSources.getValues()) {
        EXPECT_TRUE(physicalSource.getValue()->getPhysicalSourceType()->instanceOf<DefaultSourceType>()
                    || physicalSource.getValue()->getPhysicalSourceType()->instanceOf<MQTTSourceType>());
    }
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerEmptyParamsConsoleInput) {
    // given
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto commandLineParams =
        makeCommandLineArgs({"--localWorkerIp=localhost",
                             "--coordinatorPort=5000",
                             "--numWorkerThreads=5",
                             "--numberOfBuffersInGlobalBufferManager=2048",
                             "--numberOfBuffersInSourceLocalBufferPool=128",
                             "--queryCompiler.compilationStrategy=FAST",
                             "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                             "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                             "--physicalSources.type=DefaultSource",
                             "--physicalSources.numberOfBuffersToProduce=5",
                             "--physicalSources.rowLayout=false",
                             "--physicalSources.physicalSourceName=x",
                             "--physicalSources.logicalSourceName=default",
                             "--fieldNodeLocationCoordinates=23.88,-3.4"});
    // when
    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.compilationStrategy.getValue(),
              workerConfigPtr->queryCompiler.compilationStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.pipeliningStrategy.getValue(),
              workerConfigPtr->queryCompiler.pipeliningStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getValue(),
              workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testWorkerCSCVSourceConsoleInput) {
    // given
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    auto commandLineParams =
        makeCommandLineArgs({"--localWorkerIp=localhost",
                             "--coordinatorPort=5000",
                             "--numWorkerThreads=5",
                             "--numberOfBuffersInGlobalBufferManager=2048",
                             "--numberOfBuffersInSourceLocalBufferPool=128",
                             "--queryCompiler.compilationStrategy=FAST",
                             "--queryCompiler.pipeliningStrategy=OPERATOR_AT_A_TIME",
                             "--queryCompiler.outputBufferOptimizationLevel=ONLY_INPLACE_OPERATIONS_NO_FALLBACK",
                             "--physicalSources.type=CSVSource",
                             "--physicalSources.filePath=fileLoc",
                             "--physicalSources.rowLayout=false",
                             "--physicalSources.physicalSourceName=x",
                             "--physicalSources.logicalSourceName=default"});
    // when
    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    // then
    EXPECT_NE(workerConfigPtr->localWorkerIp.getValue(), workerConfigPtr->localWorkerIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->rpcPort.getValue(), workerConfigPtr->rpcPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->dataPort.getValue(), workerConfigPtr->dataPort.getDefaultValue());
    EXPECT_NE(workerConfigPtr->coordinatorPort.getValue(), workerConfigPtr->coordinatorPort.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->coordinatorIp.getValue(), workerConfigPtr->coordinatorIp.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfSlots.getValue(), workerConfigPtr->numberOfSlots.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->logLevel.getValue(), workerConfigPtr->logLevel.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInGlobalBufferManager.getValue(),
              workerConfigPtr->numberOfBuffersInGlobalBufferManager.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->numberOfBuffersPerWorker.getValue(), workerConfigPtr->numberOfBuffersPerWorker.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getValue(),
              workerConfigPtr->numberOfBuffersInSourceLocalBufferPool.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->bufferSizeInBytes.getValue(), workerConfigPtr->bufferSizeInBytes.getDefaultValue());
    EXPECT_NE(workerConfigPtr->numWorkerThreads.getValue(), workerConfigPtr->numWorkerThreads.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.compilationStrategy.getValue(),
              workerConfigPtr->queryCompiler.compilationStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.pipeliningStrategy.getValue(),
              workerConfigPtr->queryCompiler.pipeliningStrategy.getDefaultValue());
    EXPECT_NE(workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getValue(),
              workerConfigPtr->queryCompiler.outputBufferOptimizationLevel.getDefaultValue());
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), workerConfigPtr->locationCoordinates.getDefaultValue());
}

TEST_F(ConfigTest, testSourceEmptyParamsConsoleInput) {
    auto commandLineParams = makeCommandLineArgs({"type=DefaultSource",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "offsetMode=earliest"});

    PhysicalSourcePtr physicalSource1 = PhysicalSourceFactory::createFromString("", commandLineParams);
    EXPECT_EQ(physicalSource1->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource1->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource1->getPhysicalSourceType()->instanceOf<DefaultSourceType>());

    DefaultSourceTypePtr physicalSourceType1 = physicalSource1->getPhysicalSourceType()->as<DefaultSourceType>();
    EXPECT_EQ(physicalSourceType1->getSourceGatheringInterval()->getValue(),
              physicalSourceType1->getSourceGatheringInterval()->getDefaultValue());
    EXPECT_NE(physicalSourceType1->getNumberOfBuffersToProduce()->getValue(), 5u);

    auto commandLineParams1 = makeCommandLineArgs({"type=KafkaSource",
                                                   "physicalSourceName=x",
                                                   "logicalSourceName=default",
                                                   "topic=newTopic",
                                                   "connectionTimeout=100",
                                                   "brokers=testBroker",
                                                   "groupId=testId",
                                                   "offsetMode=earliest"});

    PhysicalSourcePtr physicalSource2 = PhysicalSourceFactory::createFromString("", commandLineParams1);
    EXPECT_EQ(physicalSource2->getLogicalSourceName(), "default");
    EXPECT_EQ(physicalSource2->getPhysicalSourceName(), "x");
    EXPECT_TRUE(physicalSource2->getPhysicalSourceType()->instanceOf<KafkaSourceType>());

    KafkaSourceTypePtr physicalSourceType2 = physicalSource2->getPhysicalSourceType()->as<KafkaSourceType>();
    EXPECT_NE(physicalSourceType2->getBrokers()->getValue(), physicalSourceType2->getBrokers()->getDefaultValue());
    EXPECT_EQ(physicalSourceType2->getAutoCommit()->getValue(), physicalSourceType2->getAutoCommit()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getGroupId()->getValue(), physicalSourceType2->getGroupId()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getTopic()->getValue(), physicalSourceType2->getTopic()->getDefaultValue());
    EXPECT_NE(physicalSourceType2->getConnectionTimeout()->getValue(),
              physicalSourceType2->getConnectionTimeout()->getDefaultValue());
}

TEST_F(ConfigTest, testPhysicalSourceAndGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=DefaultSource",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default"});
    // when
    PhysicalSourcePtr physicalSource1 = PhysicalSourceFactory::createFromString("", commandLineParams);
    DefaultSourceTypePtr physicalSourceType1 = physicalSource1->getPhysicalSourceType()->as<DefaultSourceType>();
    // then
    EXPECT_EQ(physicalSourceType1->getGatheringMode()->getValue(), physicalSourceType1->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType1->getGatheringMode()->getValue(), GatheringMode::getFromString("interval"));
}

TEST_F(ConfigTest, testCSVPhysicalSourceAndDefaultGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=CSVSource",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "filePath=fileLoc"});
    // when
    PhysicalSourcePtr physicalSource = PhysicalSourceFactory::createFromString("", commandLineParams);
    CSVSourceTypePtr physicalSourceType = physicalSource->getPhysicalSourceType()->as<CSVSourceType>();
    // then
    EXPECT_EQ(physicalSourceType->getGatheringMode()->getValue(), physicalSourceType->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType->getGatheringMode()->getValue(), GatheringMode::getFromString("interval"));
}

TEST_F(ConfigTest, testCSVPhysicalSourceAndAdaptiveGatheringModeWorkerConsoleInput) {
    // given
    auto commandLineParams = makeCommandLineArgs({"type=CSVSource",
                                                  "numberOfBuffersToProduce=5",
                                                  "rowLayout=false",
                                                  "physicalSourceName=x",
                                                  "logicalSourceName=default",
                                                  "filePath=fileLoc",
                                                  "sourceGatheringMode=adaptive"});
    // when
    PhysicalSourcePtr physicalSource = PhysicalSourceFactory::createFromString("", commandLineParams);
    CSVSourceTypePtr physicalSourceType = physicalSource->getPhysicalSourceType()->as<CSVSourceType>();
    // then
    EXPECT_NE(physicalSourceType->getGatheringMode()->getValue(), physicalSourceType->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(physicalSourceType->getGatheringMode()->getValue(), GatheringMode::getFromString("adaptive"));
}

TEST_F(ConfigTest, testWorkerYAMLFileWithCSVPhysicalSourceAdaptiveGatheringMode) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "workerWithAdaptiveCSVSource.yaml");
    EXPECT_TRUE(workerConfigPtr->physicalSources.size() == 1);

    auto csvSourceType = workerConfigPtr->physicalSources.getValues()[0].getValue()->getPhysicalSourceType()->as<CSVSourceType>();
    EXPECT_NE(csvSourceType->getGatheringMode()->getValue(), csvSourceType->getGatheringMode()->getDefaultValue());
    EXPECT_EQ(csvSourceType->getGatheringMode()->getValue(), GatheringMode::getFromString("adaptive"));
}

TEST_F(ConfigTest, parseWorkerOptionInCoordinatorConfigFile) {
    // This test checks if a worker configuration option can be set in the coordinator configuration file.
    // given: Set up the coordinator configuration file with the worker option numWorkerThreads.
    auto coordinatorConfigPath = getTestResourceFolder() / "coordinator.yml";
    std::ofstream coordinatorConfigFile(coordinatorConfigPath);
    coordinatorConfigFile << WORKER_CONFIG << ":" << std::endl << "  " << NUM_WORKER_THREADS_CONFIG << ": 99" << std::endl;
    coordinatorConfigFile.close();
    // given: Specify the coordinator configuration file on the command line.
    std::vector<std::string> args = {TestUtils::configPath(coordinatorConfigPath)};
    auto posixArgs = makePosixArgs(args);
    // then
    auto config = CoordinatorConfiguration::create(posixArgs.size(), posixArgs.data());
    ASSERT_EQ(config->worker.numWorkerThreads.getValue(), 99);
}

TEST_F(ConfigTest, parseWorkerOptionInWorkerConfigFileSpecifiedInCoordinatorConfigFile) {
    // This test checks if a worker configuration can be set in a worker configuration file,
    // that is specified in the option workerConfigPath in the coordinator configuration file.
    // The options set in the worker configuration file should have precedence over any worker options
    // that are set in the coordinator configuration file.
    // given: Set up the worker configuration file.
    auto workerConfigPath = getTestResourceFolder() / "worker.yml";
    std::ofstream workerConfigFile(workerConfigPath);
    workerConfigFile << NUM_WORKER_THREADS_CONFIG << ": 35" << std::endl;
    workerConfigFile.close();
    // given: Set up the coordinator configuration file.
    auto coordinatorConfigPath = getTestResourceFolder() / "coordinator.yml";
    std::ofstream coordinatorConfigFile(coordinatorConfigPath);
    coordinatorConfigFile << WORKER_CONFIG << ":" << std::endl
                          << "  " << NUM_WORKER_THREADS_CONFIG << ": 15" << std::endl
                          << "  " << NUMBER_OF_SLOTS_CONFIG << ": 25" << std::endl
                          << WORKER_CONFIG_PATH << ": " << workerConfigPath << std::endl;
    coordinatorConfigFile.close();
    // given: Specify the coordinator configuration file on the command line.
    std::vector<std::string> args = {TestUtils::configPath(coordinatorConfigPath)};
    auto posixArgs = makePosixArgs(args);
    // then: The value in the worker configuration file takes precedence.
    auto config = CoordinatorConfiguration::create(posixArgs.size(), posixArgs.data());
    ASSERT_EQ(config->worker.numWorkerThreads.getValue(), 35);
    // then: Other options set in the coordinator configuration file are still processed.
    ASSERT_EQ(config->worker.numberOfSlots.getValue(), 25);
}

TEST_F(ConfigTest, parserWorkerOptionInWorkerConfigFileSpecifiedOnCommmandLine) {
    // This test checks if a worker configuration file can be set on the command line
    // using the workerConfig option.
    // The options that are set in this worker configuration file should take precedence
    // over any worker options set in a coordinator configuration file
    // (and by implication options set in the coordinator configuration file).
    // Given: Set up the first worker configuration file.
    auto workerConfigPath1 = getTestResourceFolder() / "worker-1.yml";
    std::ofstream workerConfigFile1(workerConfigPath1);
    workerConfigFile1 << NUM_WORKER_THREADS_CONFIG << ": 66" << std::endl;
    workerConfigFile1.close();
    // Given: Set up the second worker configuration file.
    auto workerConfigPath2 = getTestResourceFolder() / "worker-2.yml";
    std::ofstream workerConfigFile2(workerConfigPath2);
    workerConfigFile2 << NUM_WORKER_THREADS_CONFIG << ": 50" << std::endl << NUMBER_OF_SLOTS_CONFIG << ": 83" << endl;
    workerConfigFile2.close();
    // Given: Set up a coordinator configuration file that references the second worker file.
    auto coordinatorConfigPath = getTestResourceFolder() / "coordinator.yml";
    std::ofstream coordinatorConfigFile(coordinatorConfigPath);
    coordinatorConfigFile << WORKER_CONFIG_PATH << ": " << workerConfigPath2 << std::endl;
    coordinatorConfigFile.close();
    // given: Specify the first worker configuration file on the command line.
    std::vector<std::string> args = {TestUtils::configPath(coordinatorConfigPath),
                                     TestUtils::workerConfigPath(workerConfigPath1)};
    auto posixArgs = makePosixArgs(args);
    // then: The value in the first worker configuration file takes precedence.
    auto config = CoordinatorConfiguration::create(posixArgs.size(), posixArgs.data());
    ASSERT_EQ(config->worker.numWorkerThreads.getValue(), 66);
    // then: Other options set in the second worker configuration file are still processed.
    ASSERT_EQ(config->worker.numberOfSlots.getValue(), 83);
}

TEST_F(ConfigTest, parseWorkerOptionOnCommandline) {
    // This test checks if a worker option can be set on the command line of the coordinator.
    // These options take precedence over any option set in a worker configuration file.
    // Given: Set up the worker configuration file.
    auto workerConfigPath = getTestResourceFolder() / "worker.yml";
    std::ofstream workerConfigFile(workerConfigPath);
    workerConfigFile << NUM_WORKER_THREADS_CONFIG << ": 59" << std::endl << NUMBER_OF_SLOTS_CONFIG << ": 28" << endl;
    workerConfigFile.close();
    // Given: Set up coordinator command line
    std::vector<std::string> args = {"--worker.numWorkerThreads=68", TestUtils::workerConfigPath(workerConfigPath)};
    auto posixArgs = makePosixArgs(args);
    // then: The value set on the command line takes precedence.
    auto config = CoordinatorConfiguration::create(posixArgs.size(), posixArgs.data());
    ASSERT_EQ(config->worker.numWorkerThreads.getValue(), 68);
    // then: Other options set in the worker configuration file are still processed.
    ASSERT_EQ(config->worker.numberOfSlots.getValue(), 28);
}

}// namespace NES
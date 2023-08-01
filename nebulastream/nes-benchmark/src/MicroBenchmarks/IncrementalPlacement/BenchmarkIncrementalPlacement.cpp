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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Operators/LogicalOperators/Sinks/NullOutputSinkDescriptor.hpp>
#include <Optimizer/Phases/GlobalQueryPlanUpdatePhase.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>
#include <WorkQueues/RequestTypes/RunQueryRequest.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <z3++.h>

using namespace NES;
using namespace NES::Benchmark;
using std::filesystem::directory_iterator;

std::chrono::nanoseconds Runtime;
NES::NesCoordinatorPtr coordinator;

TopologyManagerServicePtr topologyManagerService;
TopologyPtr topology;
SourceCatalogServicePtr sourceCatalogService;
Catalogs::Source::SourceCatalogPtr sourceCatalog;
Catalogs::UDF::UdfCatalogPtr udfCatalog;

class ErrorHandler : public Exceptions::ErrorListener {
  public:
    virtual void onFatalError(int signalNumber, std::string callstack) override {
        std::cout << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callstack;
    }

    virtual void onFatalException(std::shared_ptr<std::exception> exception, std::string callstack) override {
        std::cout << "onFatalException: exception=[" << exception->what() << "] callstack=\n" << callstack;
    }
};

/**
 * @brief Set up the physical sources for the benchmark
 * @param nesCoordinator : the coordinator shared object
 * @param noOfPhysicalSource : number of physical sources
 */
void setupSources(uint64_t noOfLogicalSource, uint64_t noOfPhysicalSource) {

    //Create source catalog service
    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(nullptr);
    sourceCatalogService = std::make_shared<SourceCatalogService>(sourceCatalog);

    //register logical stream with different schema
    NES::SchemaPtr schema1 = NES::Schema::create()
                                 ->addField("a", NES::UINT64)
                                 ->addField("b", NES::UINT64)
                                 ->addField("c", NES::UINT64)
                                 ->addField("d", NES::UINT64)
                                 ->addField("e", NES::UINT64)
                                 ->addField("f", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    NES::SchemaPtr schema2 = NES::Schema::create()
                                 ->addField("g", NES::UINT64)
                                 ->addField("h", NES::UINT64)
                                 ->addField("i", NES::UINT64)
                                 ->addField("j", NES::UINT64)
                                 ->addField("k", NES::UINT64)
                                 ->addField("l", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    NES::SchemaPtr schema3 = NES::Schema::create()
                                 ->addField("m", NES::UINT64)
                                 ->addField("n", NES::UINT64)
                                 ->addField("o", NES::UINT64)
                                 ->addField("p", NES::UINT64)
                                 ->addField("q", NES::UINT64)
                                 ->addField("r", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    NES::SchemaPtr schema4 = NES::Schema::create()
                                 ->addField("s", NES::UINT64)
                                 ->addField("t", NES::UINT64)
                                 ->addField("u", NES::UINT64)
                                 ->addField("v", NES::UINT64)
                                 ->addField("w", NES::UINT64)
                                 ->addField("x", NES::UINT64)
                                 ->addField("time1", NES::UINT64)
                                 ->addField("time2", NES::UINT64);

    //Add the logical and physical stream to the stream catalog
    uint64_t counter = 1;
    for (uint64_t j = 0; j < noOfLogicalSource; j++) {
        if (counter == 1) {
            sourceCatalogService->registerLogicalSource("example" + std::to_string(j + 1), schema1);
        } else if (counter == 2) {
            sourceCatalogService->registerLogicalSource("example" + std::to_string(j + 1), schema2);
        } else if (counter == 3) {
            sourceCatalogService->registerLogicalSource("example" + std::to_string(j + 1), schema3);
        } else if (counter == 4) {
            sourceCatalogService->registerLogicalSource("example" + std::to_string(j + 1), schema4);
            counter = 0;
        }
        counter++;

        // Add Physical topology node and stream catalog entry
        for (uint64_t i = 1; i <= noOfPhysicalSource; i++) {
            //Fetch the leaf node of the topology and add all sources to it
            auto topologyNode = topologyManagerService->findNodeWithId(5);
            auto logicalSourceName = "example" + std::to_string(j + 1);
            auto physicalSourceName = "example" + std::to_string(j + 1) + std::to_string(i);
            sourceCatalogService->registerPhysicalSource(topologyNode, physicalSourceName, logicalSourceName);
        }
    }
}

/**
 * @brief Set up the topology for the benchmark
 * @param noOfTopologyNodes : number of topology nodes
 */
void setupTopology(uint64_t noOfTopologyNodes = 5) {

    topology = Topology::create();
    topologyManagerService = std::make_shared<TopologyManagerService>(topology);
    topologyManagerService->registerNode("1",
                                         0,
                                         0,
                                         UINT16_MAX,
                                         NES::Spatial::Index::Experimental::Location(),
                                         NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION,
                                         /* isTfInstalled */ false);
    for (uint64_t i = 2; i <= noOfTopologyNodes; i++) {
        topologyManagerService->registerNode(std::to_string(i),
                                             0,
                                             0,
                                             UINT16_MAX,
                                             NES::Spatial::Index::Experimental::Location(),
                                             NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION,
                                             /* isTfInstalled */ false);
    }

    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

    auto node1 = topologyManagerService->findNodeWithId(1);
    auto node2 = topologyManagerService->findNodeWithId(2);
    auto node3 = topologyManagerService->findNodeWithId(3);
    auto node4 = topologyManagerService->findNodeWithId(4);
    auto node5 = topologyManagerService->findNodeWithId(5);

    node2->addLinkProperty(node1, linkProperty);
    node1->addLinkProperty(node2, linkProperty);

    node3->addLinkProperty(node2, linkProperty);
    node2->addLinkProperty(node3, linkProperty);

    node3->addLinkProperty(node2, linkProperty);
    node2->addLinkProperty(node3, linkProperty);

    node4->addLinkProperty(node3, linkProperty);
    node3->addLinkProperty(node4, linkProperty);

    node5->addLinkProperty(node4, linkProperty);
    node4->addLinkProperty(node5, linkProperty);

    topologyManagerService->addParent(3, 2);
    topologyManagerService->removeParent(3, 1);

    topologyManagerService->addParent(4, 3);
    topologyManagerService->removeParent(4, 1);

    topologyManagerService->addParent(5, 4);
    topologyManagerService->removeParent(5, 1);
}

/**
 * @brief Setup coordinator configuration and sources to run the experiments
 * @param queryMergerRule : the query merger rule
 * @param noOfPhysicalSources : total number of physical sources
 * @param batchSize : the batch size for query processing
 */
void setUp(uint64_t noOfLogicalSource, uint64_t noOfPhysicalSources) {
    setupTopology();
    setupSources(noOfLogicalSource, noOfPhysicalSources);
}

/**
 * @brief Split string by delimiter
 * @param input : string to split
 * @param delim : delimiter
 * @return  vector of split string
 */
std::vector<std::string> split(const std::string input, char delim) {
    std::vector<std::string> result;
    std::stringstream ss(input);
    std::string item;
    while (getline(ss, item, delim)) {
        result.push_back(item);
    }
    return result;
}

/**
 * @brief Load provided configuration file
 * @param filePath : location of the configuration file
 */
Yaml::Node loadConfigFromYAMLFile(const std::string& filePath) {

    NES_INFO("BenchmarkIncrementalPlacement: Using config file with path: " << filePath << " .");
    if (!filePath.empty() && std::filesystem::exists(filePath)) {
        try {
            Yaml::Node config = *(new Yaml::Node());
            Yaml::Parse(config, filePath.c_str());
            return config;
        } catch (std::exception& e) {
            NES_ERROR("BenchmarkIncrementalPlacement: Error while initializing configuration parameters from YAML file."
                      << e.what());
            throw e;
        }
    }
    NES_ERROR("BenchmarkIncrementalPlacement: No file path was provided or file could not be found at " << filePath << ".");
    NES_THROW_RUNTIME_ERROR("Unable to find benchmark run configuration.");
}

void compileQuery(const std::string& stringQuery,
                  uint64_t id,
                  const std::shared_ptr<QueryParsingService>& queryParsingService,
                  std::promise<QueryPlanPtr> promise) {
    auto queryplan = queryParsingService->createQueryFromCodeString(stringQuery);
    queryplan->setQueryId(id);
    promise.set_value(queryplan);
}

/**
 * @brief This benchmarks time taken in the preparation of Global Query Plan after merging @param{NO_OF_QUERIES_TO_SEND} number of queries.
 */
int main(int argc, const char* argv[]) {

    auto listener = std::make_shared<ErrorHandler>();
    Exceptions::installGlobalErrorListener(listener);

    NES::Logger::setupLogging("BenchmarkIncrementalPlacement.log", NES::LogLevel::LOG_INFO);
    std::cout << "Setup BenchmarkIncrementalPlacement test class." << std::endl;
    std::stringstream benchmarkOutput;
    benchmarkOutput << "Time,BM_Name,PlacementRule,IncrementalPlacement,Run_Num,Query_Num,Start_Time,End_Time,Total_Run_Time"
                    << std::endl;

    //Load all command line arguments
    std::map<std::string, std::string> commandLineParams;
    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(std::pair<std::string, std::string>(
            std::string(argv[i]).substr(0, std::string(argv[i]).find("=")),
            std::string(argv[i]).substr(std::string(argv[i]).find("=") + 1, std::string(argv[i]).length() - 1)));
    }

    // Location of the configuration file
    auto configPath = commandLineParams.find("--configPath");

    Yaml::Node configs;
    //Load the configuration file
    if (configPath != commandLineParams.end()) {
        configs = loadConfigFromYAMLFile(configPath->second);
    } else {
        NES_ERROR("Configuration file is not provided");
        return -1;
    }

    //Fetch base benchmark configurations
    auto logLevel = configs["LogLevel"].As<std::string>();
    auto numberOfRun = configs["NumOfRuns"].As<uint16_t>();
    auto startupSleepInterval = configs["StartupSleepIntervalInSeconds"].As<uint16_t>();
    NES::Logger::setupLogging("BM.log", magic_enum::enum_cast<LogLevel>(logLevel).value());

    //Load queries from the query set location and run the benchmark
    auto querySetLocation = configs["QuerySetLocation"].As<std::string>();
    std::vector<std::string> queries;
    //Read the input query set and load the query string in the queries vector
    std::ifstream infile(querySetLocation);
    std::string line;
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        queries.emplace_back(line);
    }

    if (queries.empty()) {
        NES_THROW_RUNTIME_ERROR("Unable to find any query");
    }

    //using thread pool to parallelize the compilation of string queries and string them in an array of query objects
    const uint32_t numOfQueries = queries.size();
    std::vector<QueryPlanPtr> queryObjects;

    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);

    //If no available thread then set number of threads to 1
    uint64_t numThreads = std::thread::hardware_concurrency();
    if (numThreads == 0) {
        NES_WARNING("No available threads. Going to use only 1 thread for parsing input queries.");
        numThreads = 1;
    }
    std::cout << "Using " << numThreads << " of threads for parallel parsing." << std::endl;

    uint64_t queryNum = 0;
    //Work till all queries are not parsed
    while (queryNum < numOfQueries) {
        std::vector<std::future<QueryPlanPtr>> futures;
        std::vector<std::thread> threadPool(numThreads);
        uint64_t threadNum;
        //Schedule queries to be parsed with #numThreads parallelism
        for (threadNum = 0; threadNum < numThreads; threadNum++) {
            //If no more query to parse
            if (queryNum >= numOfQueries) {
                break;
            }
            //Schedule thread for execution and pass a promise
            std::promise<QueryPlanPtr> promise;
            //Store the future, schedule the thread, and increment the query count
            futures.emplace_back(promise.get_future());
            threadPool.emplace_back(
                std::thread(compileQuery, queries[queryNum], queryNum + 1, queryParsingService, std::move(promise)));
            queryNum++;
        }

        //Wait for all unfinished threads
        for (auto& item : threadPool) {
            if (item.joinable()) {// if thread is not finished yet
                item.join();
            }
        }

        //Fetch the parsed query from all threads
        for (uint64_t futureNum = 0; futureNum < threadNum; futureNum++) {
            auto query = futures[futureNum].get();
            auto queryID = query->getQueryId();
            queryObjects[queryID - 1] = query;//Add the parsed query to the (queryID - 1)th index
        }
    }

    std::cout << "Parsed all queries." << std::endl;

    //Set optimizer configuration
    OptimizerConfiguration optimizerConfiguration;
    optimizerConfiguration.queryMergerRule = Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule;

    //Perform benchmark for each run configuration
    auto runConfig = configs["RunConfig"];
    for (auto entry = runConfig.Begin(); entry != runConfig.End(); entry++) {
        auto node = (*entry).second;
        auto placementStrategy = node["QueryPlacementStrategy"].As<std::string>();
        auto incrementalPlacement = node["IncrementalPlacement"].As<bool>();

        for (uint32_t run = 0; run < numberOfRun; run++) {

            sleep(startupSleepInterval);

            //Setup topology and source catalog
            setUp(26, 1);

            z3::config cfg;
            cfg.set("timeout", 1000);
            cfg.set("model", false);
            cfg.set("type_check", false);
            auto z3Context = std::make_shared<z3::context>(cfg);
            udfCatalog = Catalogs::UDF::UdfCatalog::create();
            Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
            QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
            auto globalQueryPlan = GlobalQueryPlan::create();
            auto globalQueryUpdatePhase = Optimizer::GlobalQueryPlanUpdatePhase::create(topology,
                                                                                        queryCatalogService,
                                                                                        sourceCatalog,
                                                                                        globalQueryPlan,
                                                                                        z3Context,
                                                                                        optimizerConfiguration,
                                                                                        udfCatalog);

            auto globalExecutionPlan = GlobalExecutionPlan::create();
            auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
            auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              z3Context,
                                                                              incrementalPlacement);

            //Perform steps to optimize queries
            for (uint64_t i = 0; i < numOfQueries; i++) {

                auto queryPlan = queryObjects[i];
                queryCatalogService->createNewEntry("", queryPlan, placementStrategy);
                PlacementStrategy::Value queryPlacementStrategy = PlacementStrategy::getFromString(placementStrategy);
                auto runQueryRequest = RunQueryRequest::create(queryPlan, queryPlacementStrategy);

                globalQueryUpdatePhase->execute({runQueryRequest});
                auto sharedQueryPlansToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
                NES_ASSERT(sharedQueryPlansToDeploy.size() == 1, "Shared Query Plan to deploy has to be one");
                auto startTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                bool placed =
                    queryPlacementPhase->execute(runQueryRequest->getQueryPlacementStrategy(), sharedQueryPlansToDeploy[0]);
                auto endTime =
                    std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
                        .count();
                NES_ASSERT(placed, "Placement should be successful");
                benchmarkOutput << startTime << ",BM_Name," << placementStrategy << "," << incrementalPlacement << "," << run
                                << "," << queryPlan->getQueryId() << "," << startTime << "," << endTime << ","
                                << (endTime - startTime) << std::endl;
            }
            std::cout << "Finished Run " << run;
        }
    }

    //Print the benchmark output and same it to the CSV file for further processing
    std::cout << benchmarkOutput.str();
    std::ofstream out("BenchmarkQueryMerger.csv");
    out << benchmarkOutput.str();
    out.close();
    std::cout << "benchmark finish" << std::endl;
    return 0;
}
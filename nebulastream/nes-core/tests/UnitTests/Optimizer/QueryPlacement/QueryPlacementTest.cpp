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
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class QueryPlacementTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    z3::ContextPtr z3Context;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    QueryParsingServicePtr queryParsingService;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;
    /* Will be called before any test in this class are executed. */

    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        NES_DEBUG("Setup QueryPlacementTest test case.");
        z3Context = std::make_shared<z3::context>();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }

    void setupTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
        rootNode->addNodeProperty("tf_installed", true);
        topology->setAsRoot(rootNode);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);
        sourceNode1->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);
        sourceNode2->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode2);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);

        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode2);

        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }
    void setupComplexTopologyAndStreamCatalog(std::vector<uint16_t> resources) {
        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
        rootNode->addNodeProperty("tf_installed", true);
        topology->setAsRoot(rootNode);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);
        sourceNode1->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);
        sourceNode2->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode2);

        TopologyNodePtr sourceNode3 = TopologyNode::create(4, "localhost", 123, 124, resources[1]);
        topology->addNewTopologyNodeAsChild(sourceNode2, sourceNode3);

        TopologyNodePtr sourceNode4 = TopologyNode::create(5, "localhost", 123, 124, resources[2]);
        topology->addNewTopologyNodeAsChild(sourceNode2, sourceNode4);

        std::string schema = R"(Schema::create()->addField(createField("id", UINT64))
                           ->addField(createField("SepalLengthCm", FLOAT32))
                           ->addField(createField("SepalWidthCm", FLOAT32))
                           ->addField(createField("PetalLengthCm", FLOAT32))
                           ->addField(createField("PetalWidthCm", FLOAT32))
                           ->addField(createField("SpeciesCode", UINT64));)";
        const std::string sourceName = "iris";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);

        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode2);

        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                auto op = (*qPlanItr)->as<MapLogicalOperatorNode>();
                NES_DEBUG("input schema in bytes: " << op->getInputSchema()->getSchemaSizeInBytes());
                NES_DEBUG("output schema in bytes: " << op->getOutputSchema()->getSchemaSizeInBytes());
                double schemaSizeComparison =
                    1.0 * op->getOutputSchema()->getSchemaSizeInBytes() / op->getInputSchema()->getSchemaSizeInBytes();

                op->addProperty("DMF", schemaSizeComparison);
            }
        }
    }
};

/* Test query placement with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2u);
            for (const auto& sourceOperator : sourceOperators) {
                EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2u);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2u || executionNode->getId() == 3u);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2u);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2u);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2U || executionNode->getId() == 3U);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2U);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (auto actualRootOperator : actualRootOperators) {
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with Bottom up strategy  */
TEST_F(QueryPlacementTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(1);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[0]);
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::Deployed);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(2);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, updatedSharedQMToDeploy[0]);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(planToDeploy->getQueryId());
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(planToDeploy->getQueryId());
            ASSERT_EQ(querySubPlans.size(), 2UL);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                auto actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = planToDeploy->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(planToDeploy->getQueryId());
            // map merged into querySubPlan with filter
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                EXPECT_TRUE(rootOperator->as<SinkLogicalOperatorNode>()
                                ->getSinkDescriptor()
                                ->instanceOf<Network::NetworkSinkDescriptor>());
            }
            for (const auto& sourceOperator : querySubPlan->getSourceOperators()) {
                EXPECT_TRUE(sourceOperator->getParents().size() == 1);
                auto sourceParent = sourceOperator->getParents()[0];
                EXPECT_TRUE(sourceParent->instanceOf<FilterLogicalOperatorNode>());
                auto filterParents = sourceParent->getParents();
                EXPECT_TRUE(filterParents.size() == 2);
                uint8_t distinctParents = 0;
                for (const auto& filterParent : filterParents) {
                    if (filterParent->instanceOf<MapLogicalOperatorNode>()) {
                        EXPECT_TRUE(filterParent->getParents()[0]->instanceOf<SinkLogicalOperatorNode>());
                        distinctParents += 1;
                    } else {
                        EXPECT_TRUE(filterParent->instanceOf<SinkLogicalOperatorNode>());
                        distinctParents += 2;
                    }
                }
                ASSERT_EQ(distinctParents, 3);
            }
        }
    }
}

TEST_F(QueryPlacementTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({10, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(PlanIdGenerator::getNextQueryId());

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, updatedSharedQMToDeploy[0]);
    //Mark as deployed
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::Deployed);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    // new query
    auto queryPlan3 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .map(Attribute("newNewId") = 4)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());

    queryPlan3 = queryReWritePhase->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    z3InferencePhase->execute(queryPlan3);

    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto sharedQueryPlanId = updatedSharedQMToDeploy[0]->getSharedQueryId();

    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, updatedSharedQMToDeploy[0]);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            ASSERT_EQ(querySubPlans[0]->getSinkOperators().size(), 3UL);
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            // map merged into querySubPlan with filter
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                EXPECT_TRUE(rootOperator->as<SinkLogicalOperatorNode>()
                                ->getSinkDescriptor()
                                ->instanceOf<Network::NetworkSinkDescriptor>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with Top Down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2UL);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2UL || executionNode->getId() == 3UL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

// Test manual placement
TEST_F(QueryPlacementTest, testManualPlacement) {
    setupTopologyAndSourceCatalog({4, 4, 4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, false, false, false, false},
                                                     {false, true, true, false, false},
                                                     {false, false, false, true, true}};

    NES::Optimizer::BasePlacementStrategy::pinOperators(queryPlan, topology, binaryMapping);

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::Manual, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

// Test manual placement with limited resources. The manual placement should place the operator depending on the mapping
// without considering availability of the topology nodes
TEST_F(QueryPlacementTest, testManualPlacementLimitedResources) {
    setupTopologyAndSourceCatalog({1, 1, 1});// each node only has a capacity of 1
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, false, false, false, false},
                                                     {false, true, true, false, false},
                                                     {false, false, false, true, true}};

    NES::Optimizer::ManualPlacementStrategy::pinOperators(queryPlan, topology, binaryMapping);

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::Manual, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

// Test manual placement to place expanded operators in the same topology node
TEST_F(QueryPlacementTest, testManualPlacementExpandedOperatorInASingleNode) {
    setupTopologyAndSourceCatalog({1, 1, 1});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, true, false, true, false},
                                                     {false, false, true, false, false},
                                                     {false, false, false, false, true}};

    NES::Optimizer::ManualPlacementStrategy::pinOperators(queryPlan, topology, binaryMapping);

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::Manual, sharedQueryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/**
 * Test on a linear topology with one logical source
 * Topology: sinkNode--midNode---srcNode
 * Query: SinkOp---MapOp---SourceOp
 */
//TODO: enable this test after fixing #2486
TEST_F(QueryPlacementTest, DISABLED_testIFCOPPlacement) {
    // Setup the topology
    // We are using a linear topology of three nodes:
    // srcNode -> midNode -> sinkNode
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4);
    auto midNode = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto srcNode = TopologyNode::create(2, "localhost", 4002, 5002, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode);
    topology->addNewTopologyNodeAsChild(midNode, srcNode);

    ASSERT_TRUE(sinkNode->containAsChild(midNode));
    ASSERT_TRUE(midNode->containAsChild(srcNode));

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("value2") = Attribute("value") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy(NES::PlacementStrategy::IFCOP,
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              z3Context);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    // Execute the placement
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(testQueryPlan->getQueryId())) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<MapLogicalOperatorNode>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperatorNode>());
                    // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                    if (child->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                    }
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_EQ(mapPlacementCount, 1U);
}

/**
 * Test on a branched topology with one logical source
 * Topology: sinkNode--mid1--srcNode1
 *                   \
 *                    --mid2--srcNode2
 * Query: SinkOp---MapOp---SourceOp
 */
//TODO: enable this test after fixing #2486
TEST_F(QueryPlacementTest, DISABLED_testIFCOPPlacementOnBranchedTopology) {
    // Setup the topology
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto midNode2 = TopologyNode::create(2, "localhost", 4002, 5002, 4);
    auto srcNode1 = TopologyNode::create(3, "localhost", 4003, 5003, 4);
    auto srcNode2 = TopologyNode::create(4, "localhost", 4004, 5004, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(sinkNode, midNode2);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);
    topology->addNewTopologyNodeAsChild(midNode2, srcNode2);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(sinkNode->containAsChild(midNode2));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));
    ASSERT_TRUE(midNode2->containAsChild(srcNode2));

    NES_DEBUG("QueryPlacementTest:: topology: " << topology->toString());

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode2);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("value2") = Attribute("value") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy(NES::PlacementStrategy::IFCOP,
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              z3Context);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    placementStrategy->updateGlobalExecutionPlan(testQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString=" << globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(testQueryPlan->getQueryId());

    EXPECT_EQ(executionNodes.size(), 5UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(testQueryPlan->getQueryId())) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<MapLogicalOperatorNode>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        } else if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                                   == testQueryPlan->getSourceOperators()[1]->getId()) {
                            isSource2PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperatorNode>());
                    // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                    if (child->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid =
                            executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                    } else if (child->as<SourceLogicalOperatorNode>()->getId()
                               == testQueryPlan->getSourceOperators()[1]->getId()) {
                        isSource2PlacementValid =
                            executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                    }
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
    EXPECT_EQ(mapPlacementCount, 2U);
}

/**
 * Test placement of self join query on a topology with one logical source
 * Topology: sinkNode--mid1--srcNode1(A)
 *
 * Query: SinkOp---join---SourceOp(A)
 *                    \
 *                     -----SourceOp(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testTopDownPlacementOfSelfJoinQuery) {
    // Setup the topology
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 14);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: " << topology->toString());

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64)"
                         "->addField(\"timestamp\", DataTypeFactory::createUInt64());";
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car")
                      .as("c1")
                      .joinWith(Query::from("car").as("c2"))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, true);
    queryPlacementPhase->execute(NES::PlacementStrategy::TopDown, sharedQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString=" << globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(queryId)) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperators : sourceOperators) {
                if (sourceOperators->as<SourceLogicalOperatorNode>()->getId()
                    == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperators->as<SourceLogicalOperatorNode>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
}

/**
 * Test placement of self join query on a topology with one logical source
 * Topology: sinkNode--mid1--srcNode1(A)
 *
 * Query: SinkOp---join---SourceOp(A)
 *                    \
 *                     -----SourceOp(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testBottomUpPlacementOfSelfJoinQuery) {
    // Setup the topology
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 14);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: " << topology->toString());

    // Prepare the source and schema
    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64)"
                         "->addField(\"timestamp\", DataTypeFactory::createUInt64());";
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car")
                      .as("c1")
                      .joinWith(Query::from("car").as("c2"))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, z3Context, false);
    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperators : sourceOperators) {
                if (sourceOperators->as<SourceLogicalOperatorNode>()->getId()
                    == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperators->as<SourceLogicalOperatorNode>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
}

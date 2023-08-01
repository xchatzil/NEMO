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

// clang-format off
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Windowing/CentralWindowOperator.hpp>
#include <Operators/LogicalOperators/Windowing/SliceCreationOperator.hpp>
#include <Operators/LogicalOperators/Windowing/WindowComputationOperator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <Optimizer/QueryRewrite/LogicalSourceExpansionRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/WindowAggregationDescriptor.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <iostream>
using namespace NES;
using namespace Configurations;

class DistributeWindowRuleTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    Optimizer::DistributeWindowRulePtr distributeWindowRule;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributeWindowRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributeWindowRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        // enable distributed window optimization
        auto optimizerConfiguration = Configurations::OptimizerConfiguration();
        optimizerConfiguration.performDistributedWindowOptimization = true;
        optimizerConfiguration.distributedWindowChildThreshold = 2;
        optimizerConfiguration.distributedWindowCombinerThreshold = 4;
        distributeWindowRule = Optimizer::DistributedWindowRule::create(optimizerConfiguration);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }
};

void setupSensorNodeAndSourceCatalogTwoNodes(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode1);
    Catalogs::Source::SourceCatalogEntryPtr sce2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode2);

    sourceCatalog->addPhysicalSource("default_logical", sce1);
    sourceCatalog->addPhysicalSource("default_logical", sce2);
}

void setupSensorNodeAndSourceCatalogFiveNodes(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup LogicalSourceExpansionRuleTest test case.");
    TopologyPtr topology = Topology::create();

    TopologyNodePtr physicalNode1 = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode2 = TopologyNode::create(2, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode3 = TopologyNode::create(3, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode4 = TopologyNode::create(4, "localhost", 4000, 4002, 4);
    TopologyNodePtr physicalNode5 = TopologyNode::create(5, "localhost", 4000, 4002, 4);

    NES_DEBUG("topo=" << topology->toString());

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode1);
    Catalogs::Source::SourceCatalogEntryPtr sce2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode2);
    Catalogs::Source::SourceCatalogEntryPtr sce3 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode3);
    Catalogs::Source::SourceCatalogEntryPtr sce4 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode4);
    Catalogs::Source::SourceCatalogEntryPtr sce5 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode5);

    sourceCatalog->addPhysicalSource("default_logical", sce1);
    sourceCatalog->addPhysicalSource("default_logical", sce2);
    sourceCatalog->addPhysicalSource("default_logical", sce3);
    sourceCatalog->addPhysicalSource("default_logical", sce4);
    sourceCatalog->addPhysicalSource("default_logical", sce5);
}

void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) {
    NES_INFO("Setup DistributeWindowRuleTest test case.");
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto csvSourceType = CSVSourceType::create();
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "test_stream", csvSourceType);
    LogicalSourcePtr logicalSource = LogicalSource::create("default_logical", Schema::create());
    Catalogs::Source::SourceCatalogEntryPtr sce1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    sourceCatalog->addPhysicalSource("default_logical", sce1);
}

TEST_F(DistributeWindowRuleTest, testRuleForCentralWindow) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(),
                                                                 API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    NES_DEBUG(" plan before=" << queryPlan->toString());
    // Execute

    const QueryPlanPtr updatedPlan = distributeWindowRule->apply(queryPlan);

    NES_DEBUG(" plan after=" << queryPlan->toString());
    auto windowOps = queryPlan->getOperatorByType<CentralWindowOperator>();
    ASSERT_EQ(windowOps.size(), 1u);
}

TEST_F(DistributeWindowRuleTest, testRuleForDistributedWindow) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalogTwoNodes(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45)
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(),
                                                                 API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog)->execute(queryPlan);
    NES_DEBUG(" plan before log expand=" << queryPlan->toString());
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);
    NES_DEBUG(" plan after log expand=" << queryPlan->toString());

    NES_DEBUG(" plan before window distr=" << queryPlan->toString());
    updatedPlan = distributeWindowRule->apply(queryPlan);
    NES_DEBUG(" plan after window distr=" << queryPlan->toString());

    auto compOps = queryPlan->getOperatorByType<WindowComputationOperator>();
    ASSERT_EQ(compOps.size(), 1u);

    auto sliceOps = queryPlan->getOperatorByType<SliceCreationOperator>();
    ASSERT_EQ(sliceOps.size(), 2u);
}

TEST_F(DistributeWindowRuleTest, testRuleForDistributedWindowWithMerger) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalogFiveNodes(sourceCatalog);

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("default_logical")
                      .filter(Attribute("id") < 45)
                      .window(NES::Windowing::TumblingWindow::of(NES::Windowing::TimeCharacteristic::createIngestionTime(),
                                                                 API::Seconds(10)))
                      .byKey(Attribute("id"))
                      .apply(API::Sum(Attribute("value")))
                      .sink(printSinkDescriptor);

    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog)->execute(queryPlan);
    NES_DEBUG(" plan before log expand=" << queryPlan->toString());
    auto logicalSourceExpansionRule = Optimizer::LogicalSourceExpansionRule::create(sourceCatalog, false);
    QueryPlanPtr updatedPlan = logicalSourceExpansionRule->apply(queryPlan);
    NES_DEBUG(" plan after log expand=" << queryPlan->toString());

    NES_DEBUG(" plan before window distr=" << queryPlan->toString());
    updatedPlan = distributeWindowRule->apply(queryPlan);
    NES_DEBUG(" plan after window distr=" << queryPlan->toString());

    auto compOps = queryPlan->getOperatorByType<WindowComputationOperator>();
    ASSERT_EQ(compOps.size(), 1u);

    auto sliceOps = queryPlan->getOperatorByType<SliceCreationOperator>();
    ASSERT_EQ(sliceOps.size(), 5u);
}
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
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

class SyntaxBasedCompleteQueryMergerRuleTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SyntaxBasedEqualQueryMergerRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SyntaxBasedEqualQueryMergerRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        schema = Schema::create()
                     ->addField("id", BasicType::UINT32)
                     ->addField("value", BasicType::UINT64)
                     ->addField("type", BasicType::UINT64)
                     ->addField("ts", BasicType::UINT64);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
        sourceCatalog->addLogicalSource("car", schema);
        sourceCatalog->addLogicalSource("bike", schema);
        sourceCatalog->addLogicalSource("truck", schema);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4);

        auto logicalSourceCar = sourceCatalog->getLogicalSource("car");
        auto physicalSourceCar = PhysicalSource::create("car", "testCar", DefaultSourceType::create());
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceCar, logicalSourceCar, sourceNode1);
        sourceCatalog->addPhysicalSource("car", sourceCatalogEntry1);

        auto logicalSourceBike = sourceCatalog->getLogicalSource("bike");
        auto physicalSourceBike = PhysicalSource::create("bike", "testBike", DefaultSourceType::create());
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceBike, logicalSourceBike, sourceNode1);
        sourceCatalog->addPhysicalSource("bike", sourceCatalogEntry2);

        auto logicalSourceTruck = sourceCatalog->getLogicalSource("truck");
        auto physicalSourceTruck = PhysicalSource::create("truck", "testTruck", DefaultSourceType::create());
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry3 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceCar, logicalSourceCar, sourceNode1);
        sourceCatalog->addPhysicalSource("truck", sourceCatalogEntry3);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }
};

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with same queryIdAndCatalogEntryMapping
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingEqualQueries) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car")
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Children : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2Children : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1Children, sink2Children);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with same queryIdAndCatalogEntryMapping with multiple same source
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingEqualQueriesWithMultipleSameSources) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();

    auto sourceOperator11 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto sourceOperator21 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto sinkOperator11 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator11->addChild(sourceOperator11);
    sinkOperator11->addChild(sourceOperator21);

    QueryPlanPtr queryPlan1 = QueryPlan::create();
    queryPlan1->addRootOperator(sinkOperator11);
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto sourceOperator12 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto sourceOperator22 = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto sinkOperator12 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator12->addChild(sourceOperator12);
    sinkOperator12->addChild(sourceOperator22);

    QueryPlanPtr queryPlan2 = QueryPlan::create();
    queryPlan2->addRootOperator(sinkOperator12);
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1GQNChild : updatedRootOperators1[0]->getChildren()) {
        bool found = false;
        for (const auto& sink2GQNChild : updatedRootOperators1[1]->getChildren()) {
            if (sink1GQNChild->equal(sink2GQNChild)) {
                found = true;
            }
        }
        EXPECT_TRUE(found);
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different source
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentSources) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("truck").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with same queryIdAndCatalogEntryMapping with unionWith operators
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithUnionOperators) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("truck");
    Query query1 = Query::from("car")
                       .unionWith(subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Children : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2Children : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1Children, sink2Children);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with queryIdAndCatalogEntryMapping with different order of unionWith operator children
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithMergeOperatorChildrenOrder) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("car");
    Query query1 = Query::from("truck")
                       .unionWith(subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 1);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    EXPECT_TRUE(updatedSharedQueryPlan1);

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 2);

    for (const auto& sink1Children : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2Children : updatedRootOperators1[1]->getChildren()) {
            EXPECT_EQ(sink1Children, sink2Children);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with queryIdAndCatalogEntryMapping with unionWith operators but different children
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithMergeOperatorsWithDifferentChildren) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query subQuery1 = Query::from("bike");
    Query query1 = Query::from("truck")
                       .unionWith(subQuery1)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query subQuery2 = Query::from("truck");
    Query query2 = Query::from("car")
                       .unionWith(subQuery2)
                       .map(Attribute("value") = 40)
                       .filter(Attribute("id") < 45)
                       .sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different filters
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentFilters) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 45).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different filters
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentFiltersField) {
    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id1") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different map
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentMapAttribute) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value1") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different map
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentMapValue) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query1 = Query::from("car").map(Attribute("value") = 40).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    Query query2 = Query::from("car").map(Attribute("value") = 50).filter(Attribute("id") < 40).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    EXPECT_TRUE(updatedSharedQMToDeploy.size() == 2);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto updatedSharedQueryPlan2 = updatedSharedQMToDeploy[1]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_TRUE(updatedRootOperators1.size() == 1);
    auto updatedRootOperators2 = updatedSharedQueryPlan2->getRootOperators();
    EXPECT_TRUE(updatedRootOperators2.size() == 1);

    //assert
    for (const auto& sink1ChildOperator : updatedRootOperators1[0]->getChildren()) {
        for (const auto& sink2ChildOperator : updatedRootOperators2[0]->getChildren()) {
            EXPECT_NE(sink1ChildOperator, sink2ChildOperator);
        }
    }
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different windows
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentWindows) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Minutes(66), Minutes(66));
    auto aggregation1 = Sum(Attribute("value"));
    Query query1 = Query::from("car").window(windowType1).byKey(Attribute("type")).apply(aggregation1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Seconds(3960), Seconds(3960));
    auto aggregation2 = Sum(Attribute("value"));
    Query query2 = Query::from("car").window(windowType2).byKey(Attribute("type")).apply(aggregation2).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    auto rewriteRule = Optimizer::QueryRewritePhase::create(true);
    queryPlan1 = rewriteRule->execute(queryPlan1);
    queryPlan2 = rewriteRule->execute(queryPlan2);

    auto topoSpecificRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                    sourceCatalog,
                                                                                    Configurations::OptimizerConfiguration());
    queryPlan1 = topoSpecificRewrite->execute(queryPlan1);
    queryPlan2 = topoSpecificRewrite->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getAllSharedQueryPlans();
    EXPECT_EQ(updatedSharedQMToDeploy.size(), 1u);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_EQ(updatedRootOperators1.size(), 2u);
}

/**
 * @brief Test applying SyntaxBasedEqualQueryMergerRule on Global query plan with two queryIdAndCatalogEntryMapping with different windows
 */
TEST_F(SyntaxBasedCompleteQueryMergerRuleTest, testMergingQueriesWithDifferentWindowsWithoutKeyBy) {

    // Prepare
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto windowType1 = SlidingWindow::of(EventTime(Attribute("ts")), Minutes(66), Minutes(66));
    auto aggregation1 = Sum(Attribute("value"));
    Query query1 = Query::from("car").window(windowType1).apply(aggregation1).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator1 = queryPlan1->getSinkOperators()[0];
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    queryPlan1->setQueryId(queryId1);

    auto windowType2 = SlidingWindow::of(EventTime(Attribute("ts")), Seconds(3960), Seconds(3960));
    auto aggregation2 = Sum(Attribute("value"));
    Query query2 = Query::from("car").window(windowType2).apply(aggregation2).sink(printSinkDescriptor);
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();
    SinkLogicalOperatorNodePtr sinkOperator2 = queryPlan2->getSinkOperators()[0];
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    queryPlan2->setQueryId(queryId2);

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan2);

    auto rewriteRule = Optimizer::QueryRewritePhase::create(true);
    queryPlan1 = rewriteRule->execute(queryPlan1);
    queryPlan2 = rewriteRule->execute(queryPlan2);

    auto topoSpecificRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(Topology::create(),
                                                                                    sourceCatalog,
                                                                                    Configurations::OptimizerConfiguration());
    queryPlan1 = topoSpecificRewrite->execute(queryPlan1);
    queryPlan2 = topoSpecificRewrite->execute(queryPlan2);

    auto globalQueryPlan = GlobalQueryPlan::create();
    globalQueryPlan->addQueryPlan(queryPlan1);
    globalQueryPlan->addQueryPlan(queryPlan2);

    //execute
    auto syntaxBasedEqualQueryMergerRule = Optimizer::SyntaxBasedCompleteQueryMergerRule::create();
    syntaxBasedEqualQueryMergerRule->apply(globalQueryPlan);

    //assert
    auto updatedSharedQMToDeploy = globalQueryPlan->getAllSharedQueryPlans();
    EXPECT_EQ(updatedSharedQMToDeploy.size(), 1u);

    auto updatedSharedQueryPlan1 = updatedSharedQMToDeploy[0]->getQueryPlan();

    //assert that the sink operators have same up-stream operator
    auto updatedRootOperators1 = updatedSharedQueryPlan1->getRootOperators();
    EXPECT_EQ(updatedRootOperators1.size(), 2u);
}

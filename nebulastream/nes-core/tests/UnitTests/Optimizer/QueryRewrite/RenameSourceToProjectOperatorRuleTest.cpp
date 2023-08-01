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
// clang-format on
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameSourceOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryRewrite/RenameSourceToProjectOperatorRule.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

using namespace NES;

class RenameSourceToProjectOperatorRuleTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RenameSourceToProjectOperatorRuleTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RenameSourceToProjectOperatorRuleTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        schema = Schema::create()->addField("a", BasicType::UINT32)->addField("b", BasicType::UINT32);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
    }

    void setupSensorNodeAndSourceCatalog(const Catalogs::Source::SourceCatalogPtr& sourceCatalog) const {
        NES_INFO("Setup FilterPushDownTest test case.");
        TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
        PhysicalSourcePtr physicalSource = PhysicalSource::create("x", "x1");
        LogicalSourcePtr logicalSource = LogicalSource::create("x", schema);
        Catalogs::Source::SourceCatalogEntryPtr sce =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
        sourceCatalog->addLogicalSource("src", schema);
        sourceCatalog->addPhysicalSource("src", sce);
    }
};

TEST_F(RenameSourceToProjectOperatorRuleTest, testAddingSingleSourceRenameOperator) {

    // Prepare
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src").map(Attribute("b") = Attribute("b") + Attribute("a")).as("x").sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameSourceOperators = queryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(!renameSourceOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameSourceToProjectOperatorRule = Optimizer::RenameSourceToProjectOperatorRule::create();
    auto updatedQueryPlan = renameSourceToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameSourceOperators = updatedQueryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(renameSourceOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 1);
}

TEST_F(RenameSourceToProjectOperatorRuleTest, testAddingMultipleSourceRenameOperator) {

    // Prepare
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query =
        Query::from("src").as("y").map(Attribute("b") = Attribute("b") + Attribute("a")).as("x").sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameSourceOperators = queryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(!renameSourceOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameSourceToProjectOperatorRule = Optimizer::RenameSourceToProjectOperatorRule::create();
    auto updatedQueryPlan = renameSourceToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameSourceOperators = updatedQueryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(renameSourceOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 2);
}

TEST_F(RenameSourceToProjectOperatorRuleTest, testAddingSourceRenameOperatorWithProject) {

    // Prepare
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    setupSensorNodeAndSourceCatalog(sourceCatalog);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    Query query = Query::from("src")
                      .project(Attribute("b"), Attribute("a"))
                      .map(Attribute("b") = Attribute("b") + Attribute("a"))
                      .as("x")
                      .sink(printSinkDescriptor);
    const QueryPlanPtr queryPlan = query.getQueryPlan();

    auto renameSourceOperators = queryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(!renameSourceOperators.empty());

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    typeInferencePhase->execute(queryPlan);

    auto renameSourceToProjectOperatorRule = Optimizer::RenameSourceToProjectOperatorRule::create();
    auto updatedQueryPlan = renameSourceToProjectOperatorRule->apply(queryPlan);

    typeInferencePhase->execute(updatedQueryPlan);

    renameSourceOperators = updatedQueryPlan->getOperatorByType<RenameSourceOperatorNode>();
    EXPECT_TRUE(renameSourceOperators.empty());

    auto projectOperators = updatedQueryPlan->getOperatorByType<ProjectionLogicalOperatorNode>();
    EXPECT_TRUE(projectOperators.size() == 2);
}

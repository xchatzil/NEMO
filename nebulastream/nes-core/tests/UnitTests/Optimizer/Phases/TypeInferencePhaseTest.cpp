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
#include <API/AttributeField.hpp>
#include <API/QueryAPI.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/BatchJoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapJavaUdfLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/ProjectionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/RenameSourceOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/JavaUdfDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/TimeCharacteristic.hpp>
#include <Windowing/WindowActions/LazyNestLoopJoinTriggerActionDescriptor.hpp>
#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>

using namespace NES::API;
using namespace NES::Windowing;

namespace NES {

class TypeInferencePhaseTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    Catalogs::UDF::UdfCatalogPtr udfCatalog = Catalogs::UDF::UdfCatalog::create();
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TypeInferencePhaseTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TypeInferencePhaseTest test class.");
    }
};

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlan) {
    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f1") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = phase->execute(plan);

    // we just access the old references
    auto expectedInputSchema = Schema::create();
    expectedInputSchema->addField("default_logical$f1", BasicType::INT32);
    expectedInputSchema->addField("default_logical$f2", BasicType::INT8);

    EXPECT_TRUE(source->getOutputSchema()->equals(expectedInputSchema));

    auto mappedSchema = Schema::create();
    mappedSchema->addField("default_logical$f1", BasicType::INT32);
    mappedSchema->addField("default_logical$f2", BasicType::INT8);
    mappedSchema->addField("default_logical$f3", BasicType::INT8);

    NES_DEBUG("first=" << map->getOutputSchema()->toString() << " second=" << mappedSchema->toString());
    EXPECT_TRUE(map->getOutputSchema()->equals(mappedSchema));
    EXPECT_TRUE(sink->getOutputSchema()->equals(mappedSchema));
}

/**
 * @brief In this test we infer the output and input schemas of each operator in a query.
 */
TEST_F(TypeInferencePhaseTest, inferWindowQuery) {

    auto query = Query::from("default_logical")
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(""));

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    NES_DEBUG(resultPlan->getSinkOperators()[0]->getOutputSchema()->toString());
    // we just access the old references
    ASSERT_EQ(resultPlan->getSinkOperators()[0]->getOutputSchema()->getSize(), 5UL);
}

/**
 * @brief In this test we try to infer the output and input scheas of an invalid query. This should fail.
 */
TEST_F(TypeInferencePhaseTest, inferQueryPlanError) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f3") = Attribute("f3") * 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
 * @brief In this test we ensure that the source descriptor is correctly replaced, such that the schema can be propagated.
 */
TEST_F(TypeInferencePhaseTest, inferQuerySourceReplace) {

    auto query = Query::from("default_logical").map(Attribute("f3") = Attribute("id")++).sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("default_logical$id", BasicType::UINT32)
                            ->addField("default_logical$value", BasicType::UINT64)
                            ->addField("default_logical$f3", BasicType::UINT32);

    NES_INFO(sink->getOutputSchema()->toString());

    EXPECT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

/**
 * @brief In this test we ensure that the schema can be propagated properly when unionWith operator is present.
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithMergeOperator) {

    Query subQuery = Query::from("default_logical");
    auto query = Query::from("default_logical")
                     .unionWith(subQuery)
                     .map(Attribute("f3") = Attribute("id")++)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sink = plan->getSinkOperators()[0];

    auto resultSchema = Schema::create()
                            ->addField("default_logical$id", BasicType::UINT32)
                            ->addField("default_logical$value", BasicType::UINT64)
                            ->addField("default_logical$f3", BasicType::UINT32);

    NES_INFO(sink->getOutputSchema()->toString());
    EXPECT_TRUE(sink->getOutputSchema()->equals(resultSchema));
}

/**
 * @brief In this test we test the rename operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryRenameBothAttributes) {

    auto inputSchema = Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8);

    auto query = Query::from("default_logical")
                     .project(Attribute("f3").as("f5"))
                     .map(Attribute("f4") = Attribute("f5") * 42)
                     .sink(FileSinkDescriptor::create(""));

    auto plan = query.getQueryPlan();

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalSourcePtr physicalSource = PhysicalSource::create("x", "x1");
    LogicalSourcePtr logicalSource = LogicalSource::create("x", inputSchema);

    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("default_logical", sce);
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
 * @brief In this test we test the as operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryRenameOneAttribute) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto query = Query::from("default_logical")
                     .map(Attribute("f3") = Attribute("f3") * 42)
                     .project(Attribute("f3").as("f4"))
                     .sink(FileSinkDescriptor::create(""));

    auto plan = query.getQueryPlan();

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalSourcePtr physicalSource = PhysicalSource::create("x", "x1");
    LogicalSourcePtr logicalSource = LogicalSource::create("x", inputSchema);

    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);
    sourceCatalog->addPhysicalSource("default_logical", sce);
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    ASSERT_ANY_THROW(phase->execute(plan));
}

/**
     * @brief In this test we test the as operator
     */
TEST_F(TypeInferencePhaseTest, inferQueryMapAssignment) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);

    auto source = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("default_logical"));
    auto map = LogicalOperatorFactory::createMapOperator(Attribute("f4") = 42);
    auto sink = LogicalOperatorFactory::createSinkOperator(FileSinkDescriptor::create(""));

    auto plan = QueryPlan::create(source);
    plan->appendOperatorAsNewRoot(map);
    plan->appendOperatorAsNewRoot(sink);

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto maps = plan->getOperatorByType<MapLogicalOperatorNode>();
    phase->execute(plan);
    NES_DEBUG("result schema is=" << maps[0]->getOutputSchema()->toString());
    //we have to forbit the renaming of the attribute in the assignment statement of the map
    ASSERT_NE(maps[0]->getOutputSchema()->getIndex("f4"), 2UL);
}

/**
 * @brief In this test we test the rename operator inside a project operator
 */
TEST_F(TypeInferencePhaseTest, inferTypeForSimpleQuery) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f2"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f1"));
}

/**
 * @brief In this test we test the power operator
 */
TEST_F(TypeInferencePhaseTest, inferTypeForPowerOperatorQuery) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::FLOAT64);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .map(Attribute("powIntInt") = POWER(Attribute("f1"), 2))
                     .map(Attribute("powFloatInt") = POWER(Attribute("f2"), 2))
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 4);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$powIntInt"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$powFloatInt"));

    auto f1 = mapOutputSchema->get("default_logical$f1");
    auto f2 = mapOutputSchema->get("default_logical$f2");
    auto powIntInt = mapOutputSchema->get("default_logical$powIntInt");
    auto powFloatInt = mapOutputSchema->get("default_logical$powFloatInt");

    EXPECT_TRUE(f1->getDataType()->isEquals(DataTypeFactory::createInt32()));
    EXPECT_TRUE(f2->getDataType()->isEquals(DataTypeFactory::createDouble()));
    EXPECT_TRUE(powIntInt->getDataType()->isEquals(DataTypeFactory::createInt64()));
    EXPECT_TRUE(powFloatInt->getDataType()->isEquals(DataTypeFactory::createDouble()));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 4);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f2"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("powIntInt"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("powFloatInt"));
}

/**
 * @brief In this test we test the type inference for query with Project operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithProject) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .project(Attribute("f1").as("f3"), Attribute("f2").as("f4"))
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f4"));
}

/**
 * @brief In this test we test the type inference for query with Source Rename operator
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameSource) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .map(Attribute("f1") = Attribute("f1") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f1"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f2"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f2"));
}

/**
 * @brief In this test we test the type inference for query with Source Rename and Project operators
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameSourceAndProject) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").as("f3"), Attribute("f2").as("f4"))
                     .map(Attribute("f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f4"));
}

/**
 * @brief In this test we test the type inference for query with fully qualified attribute names
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithPartlyOrFullyQualifiedAttributes) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("default_logical$f2") < 42)
                     .map(Attribute("f1") = Attribute("f2") + 2)
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f2"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f2"));
}

/**
 * @brief In this test we test the type inference for query with Source Rename and Project operators with fully qualified source name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameSourceAndProjectWithFullyQualifiedNames) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto query = Query::from("default_logical")
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").as("f3"), Attribute("f2").as("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("f4"));
}

/**
 * @brief In this test we test the type inference for query with Merge, Source Rename and Project operators with fully qualified source name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameSourceAndProjectWithFullyQualifiedNamesAndMergeOperator) {

    auto inputSchema = Schema::create();
    inputSchema->addField("f1", BasicType::INT32);
    inputSchema->addField("f2", BasicType::INT8);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto subQuery = Query::from("default_logical");

    auto query = Query::from("default_logical")
                     .unionWith(subQuery)
                     .filter(Attribute("f2") < 42)
                     .project(Attribute("f1").as("f3"), Attribute("f2").as("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto mergeOperator = plan->getOperatorByType<UnionLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr mergeOutputSchema = mergeOperator[0]->getOutputSchema();
    EXPECT_TRUE(mergeOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mergeOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(mergeOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    EXPECT_TRUE(filterOutputSchema->fields.size() == 2);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f4"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("x$default_logical$f4"));
}

/**
 * @brief In this test we test the type inference for query with Join, Source Rename and Project operators with fully qualified source name
 */
TEST_F(TypeInferencePhaseTest, inferQueryWithRenameSourceAndProjectWithFullyQualifiedNamesAndJoinOperator) {
    auto inputSchema =
        Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8)->addField("ts", BasicType::INT64);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto subQuery = Query::from("default_logical").as("x");
    auto query = Query::from("default_logical")
                     .as("y")
                     .joinWith(subQuery)
                     .where(Attribute("f1"))
                     .equalsTo(Attribute("f1"))
                     .window(windowType1)
                     .filter(Attribute("x$default_logical$f2") < 42)
                     .project(Attribute("x$default_logical$f1").as("f3"), Attribute("y$default_logical$f2").as("f4"))
                     .map(Attribute("default_logical$f3") = Attribute("f4") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$ts"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << filterOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(filterOutputSchema->fields.size() == 9);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("x$default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("x$default_logical$f1"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("x$default_logical$ts"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("y$default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("y$default_logical$f1"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("y$default_logical$ts"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("yx$start"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("yx$end"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("yx$key"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("y$default_logical$f4"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("default_logical$f3"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("y$default_logical$f4"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$x$default_logical$f3"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$y$default_logical$f4"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("x$default_logical$f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("y$default_logical$f4"));
}

/**
 * @brief In this test we test the type inference for query with two Joins, Source Rename, map, and Project operators with fully qualified source name
 */
TEST_F(TypeInferencePhaseTest, testInferQueryWithMultipleJoins) {
    auto inputSchema =
        Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8)->addField("ts", BasicType::INT64);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto inputSchema2 =
        Schema::create()->addField("f3", BasicType::INT32)->addField("f4", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical2", inputSchema2);

    auto inputSchema3 =
        Schema::create()->addField("f5", BasicType::INT32)->addField("f6", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical3", inputSchema3);

    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto subQuery = Query::from("default_logical2");
    auto subQuery2 = Query::from("default_logical3");
    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("f1"))
                     .equalsTo(Attribute("f3"))
                     .window(windowType1)
                     .joinWith(subQuery2)
                     .where(Attribute("f5"))
                     .equalsTo(Attribute("f3"))
                     .window(windowType2)
                     .filter(Attribute("default_logical$f1") < 42)
                     .project(Attribute("default_logical$f1").as("f23"), Attribute("default_logical2$f3").as("f44"))
                     .map(Attribute("f23") = Attribute("f44") + 2)
                     .as("x")
                     .sink(FileSinkDescriptor::create(""));
    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto filterOperator = plan->getOperatorByType<FilterLogicalOperatorNode>();
    auto mapOperator = plan->getOperatorByType<MapLogicalOperatorNode>();
    auto projectOperator = plan->getOperatorByType<ProjectionLogicalOperatorNode>();
    auto renameSourceOperator = plan->getOperatorByType<RenameSourceOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    NES_DEBUG("expected src0= " << sourceOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical3$ts"));

    SchemaPtr sourceOutputSchema2 = sourceOperator[1]->getOutputSchema();
    NES_DEBUG("expected src2= " << sourceOperator[1]->getOutputSchema()->toString());
    EXPECT_TRUE(sourceOutputSchema2->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical$ts"));

    SchemaPtr filterOutputSchema = filterOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << filterOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(filterOutputSchema->fields.size() == 15);
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3default_logicaldefault_logical2$start"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3default_logicaldefault_logical2$end"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3default_logicaldefault_logical2$key"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical3$ts"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logicaldefault_logical2$start"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logicaldefault_logical2$end"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logicaldefault_logical2$key"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical$ts"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical2$f3"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical2$f4"));
    EXPECT_TRUE(filterOutputSchema->hasFieldName("default_logical2$ts"));

    SchemaPtr projectOutputSchema = projectOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << projectOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(projectOutputSchema->fields.size() == 2);
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical$f23"));
    EXPECT_TRUE(projectOutputSchema->hasFieldName("default_logical2$f44"));

    SchemaPtr mapOutputSchema = mapOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << mapOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(mapOutputSchema->fields.size() == 2);
    EXPECT_TRUE(mapOutputSchema->hasFieldName("f23"));
    EXPECT_TRUE(mapOutputSchema->hasFieldName("f44"));

    SchemaPtr renameSourceOutputSchema = renameSourceOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << renameSourceOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(renameSourceOutputSchema->fields.size() == 2);
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical2$f44"));
    EXPECT_TRUE(renameSourceOutputSchema->hasFieldName("x$default_logical$f23"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 2);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("x$default_logical$f23"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("x$default_logical2$f44"));
}

/**
 * @brief In this test we infer the output and input schemas of each operator in a multi window query
 */
TEST_F(TypeInferencePhaseTest, inferMultiWindowQuery) {
    auto query = Query::from("default_logical")
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .byKey(Attribute("value"))
                     .apply(Sum(Attribute("id")))
                     .sink(FileSinkDescriptor::create(""));

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    auto windows = resultPlan->getOperatorByType<WindowOperatorNode>();

    NES_DEBUG("win1=" << windows[0]->getOutputSchema()->toString());
    EXPECT_TRUE(windows[0]->getOutputSchema()->fields.size() == 5);
    EXPECT_TRUE(windows[0]->getOutputSchema()->hasFieldName("default_logical$start"));
    EXPECT_TRUE(windows[0]->getOutputSchema()->hasFieldName("default_logical$end"));
    EXPECT_TRUE(windows[0]->getOutputSchema()->hasFieldName("default_logical$cnt"));
    EXPECT_TRUE(windows[0]->getOutputSchema()->hasFieldName("default_logical$value"));
    EXPECT_TRUE(windows[0]->getOutputSchema()->hasFieldName("default_logical$id"));

    NES_DEBUG("win2=" << windows[1]->getOutputSchema()->toString());
    EXPECT_TRUE(windows[1]->getOutputSchema()->fields.size() == 5);
    EXPECT_TRUE(windows[1]->getOutputSchema()->hasFieldName("default_logical$start"));
    EXPECT_TRUE(windows[1]->getOutputSchema()->hasFieldName("default_logical$end"));
    EXPECT_TRUE(windows[1]->getOutputSchema()->hasFieldName("default_logical$cnt"));
    EXPECT_TRUE(windows[1]->getOutputSchema()->hasFieldName("default_logical$value"));
    EXPECT_TRUE(windows[1]->getOutputSchema()->hasFieldName("default_logical$id"));

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 5);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$cnt"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$value"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$id"));
}

/**
 * @brief In this test we infer the output and input schemas of each operator window join query
 */
TEST_F(TypeInferencePhaseTest, inferWindowJoinQuery) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema =
        Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto inputSchema2 =
        Schema::create()->addField("f3", BasicType::INT32)->addField("f4", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical2", inputSchema2);

    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto subQuery = Query::from("default_logical2");

    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("f1"))
                     .equalsTo(Attribute("f3"))
                     .window(windowType1)
                     .window(TumblingWindow::of(TimeCharacteristic::createIngestionTime(), Seconds(10)))
                     .byKey(Attribute("default_logical$f1"))
                     .apply(Sum(Attribute("default_logical2$f3")))
                     .sink(FileSinkDescriptor::create(""));

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    NES_DEBUG(resultPlan->getSinkOperators()[0]->getOutputSchema()->toString());
    // we just access the old references
    ASSERT_EQ(resultPlan->getSinkOperators()[0]->getOutputSchema()->getSize(), 5U);

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 5);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$cnt"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical2$f3"));
}

/**
 * @brief Inference test for query with manually inserted batch Join.
 */
TEST_F(TypeInferencePhaseTest, inferBatchJoinQueryManuallyInserted) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");

    SchemaPtr schemaProbeSide =
        Schema::create()
            ->addField("id1", BasicType::INT64)
            ->addField("one", BasicType::INT64)
            ->addField("timestamp",
                       BasicType::INT64)// todo should be called value. only called timestamp for watermark operator to work.
        ;
    sourceCatalog->addLogicalSource("probe", schemaProbeSide);

    SchemaPtr schemaBuildSide =
        Schema::create()
            ->addField("id2", BasicType::INT64)
            ->addField("timestamp",
                       BasicType::INT64)// todo should be called value. only called timestamp for watermark operator to work.
        ;
    sourceCatalog->addLogicalSource("build", schemaBuildSide);

    auto subQuery = Query::from("build");

    auto query = Query::from("probe")
                     .joinWith(subQuery)
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(FileSinkDescriptor::create(""));

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto typeInferencePhaseContext = Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    JoinLogicalOperatorNodePtr joinOp = queryPlan->getOperatorByType<JoinLogicalOperatorNode>()[0];
    Experimental::BatchJoinLogicalOperatorNodePtr batchJoinOp;
    {
        Join::Experimental::LogicalBatchJoinDefinitionPtr batchJoinDef = Join::Experimental::LogicalBatchJoinDefinition::create(
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "id1")->as<FieldAccessExpressionNode>(),
            FieldAccessExpressionNode::create(DataTypeFactory::createInt64(), "id2")->as<FieldAccessExpressionNode>(),
            1,
            1);

        batchJoinOp =
            LogicalOperatorFactory::createBatchJoinOperator(batchJoinDef)->as<Experimental::BatchJoinLogicalOperatorNode>();
    }
    joinOp->replace(batchJoinOp);
    ASSERT_TRUE(batchJoinOp->inferSchema(typeInferencePhaseContext));

    for (auto wmaOp : queryPlan->getOperatorByType<WatermarkAssignerLogicalOperatorNode>()) {
        ASSERT_TRUE(wmaOp->removeAndJoinParentAndChildren());
    }

    // after cutting the wmaOps, infer schema of the operator tree again
    ASSERT_TRUE(queryPlan->getSinkOperators()[0]->inferSchema(typeInferencePhaseContext));

    auto sinkOperator = queryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("inferred output schema = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 5);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$id1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$one"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("build$id2"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("build$timestamp"));
}

/**
 * @brief Inference test for query with batch Join.
 */
TEST_F(TypeInferencePhaseTest, inferBatchJoinQuery) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");

    SchemaPtr schemaProbeSide =
        Schema::create()
            ->addField("id1", BasicType::INT64)
            ->addField("one", BasicType::INT64)
            ->addField("timestamp",
                       BasicType::INT64)// todo should be called value. only called timestamp for watermark operator to work.
        ;
    sourceCatalog->addLogicalSource("probe", schemaProbeSide);

    SchemaPtr schemaBuildSide =
        Schema::create()
            ->addField("id2", BasicType::INT64)
            ->addField("timestamp",
                       BasicType::INT64)// todo should be called value. only called timestamp for watermark operator to work.
        ;
    sourceCatalog->addLogicalSource("build", schemaBuildSide);

    auto subQuery = Query::from("build");

    auto query = Query::from("probe")
                     .batchJoinWith(subQuery)
                     .where(Attribute("id1"))
                     .equalsTo(Attribute("id2"))
                     .sink(FileSinkDescriptor::create(""));

    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    auto queryPlan = typeInferencePhase->execute(query.getQueryPlan());

    auto sinkOperator = queryPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("inferred output schema = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 5);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$id1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$one"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("probe$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("build$id2"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("build$timestamp"));
}

/**
 * @brief In this test we test the type inference for query with two Joins, Source Rename, map, and Project operators with fully qualified source name
 */
TEST_F(TypeInferencePhaseTest, testJoinOnFourSources) {
    auto inputSchema =
        Schema::create()->addField("f1", BasicType::INT32)->addField("f2", BasicType::INT8)->addField("ts", BasicType::INT64);
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->removeLogicalSource("default_logical");
    sourceCatalog->addLogicalSource("default_logical", inputSchema);

    auto inputSchema2 =
        Schema::create()->addField("f3", BasicType::INT32)->addField("f4", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical2", inputSchema2);

    auto inputSchema3 =
        Schema::create()->addField("f5", BasicType::INT32)->addField("f6", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical3", inputSchema3);

    auto inputSchema4 =
        Schema::create()->addField("f7", BasicType::INT32)->addField("f8", BasicType::INT8)->addField("ts", BasicType::INT64);
    sourceCatalog->addLogicalSource("default_logical4", inputSchema4);

    auto windowType1 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto windowType2 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto windowType3 = TumblingWindow::of(EventTime(Attribute("ts")), Milliseconds(4));
    auto subQuery = Query::from("default_logical2");
    auto subQuery3 = Query::from("default_logical4");

    auto subQuery2 =
        Query::from("default_logical3").joinWith(subQuery3).where(Attribute("f5")).equalsTo(Attribute("f7")).window(windowType3);
    auto query = Query::from("default_logical")
                     .joinWith(subQuery)
                     .where(Attribute("f1"))
                     .equalsTo(Attribute("f3"))
                     .window(windowType1)
                     .joinWith(subQuery2)
                     .where(Attribute("f1"))
                     .equalsTo(Attribute("f5"))
                     .window(windowType2)
                     .sink(FileSinkDescriptor::create(""));

    auto plan = query.getQueryPlan();

    auto phase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    plan = phase->execute(plan);
    auto sourceOperator = plan->getOperatorByType<SourceLogicalOperatorNode>();
    auto joinOperators = plan->getOperatorByType<JoinLogicalOperatorNode>();
    auto sinkOperator = plan->getOperatorByType<SinkLogicalOperatorNode>();

    SchemaPtr sourceOutputSchema = sourceOperator[0]->getOutputSchema();
    NES_DEBUG("expected src0= " << sourceOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sourceOutputSchema->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sourceOutputSchema->hasFieldName("default_logical$ts"));

    SchemaPtr sourceOutputSchema2 = sourceOperator[1]->getOutputSchema();
    NES_DEBUG("expected src2= " << sourceOperator[1]->getOutputSchema()->toString());
    EXPECT_TRUE(sourceOutputSchema2->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical2$f3"));
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical2$f4"));
    EXPECT_TRUE(sourceOutputSchema2->hasFieldName("default_logical2$ts"));

    SchemaPtr sourceOutputSchema3 = sourceOperator[2]->getOutputSchema();
    NES_DEBUG("expected src3= " << sourceOperator[2]->getOutputSchema()->toString());
    EXPECT_TRUE(sourceOutputSchema3->fields.size() == 3);
    EXPECT_TRUE(sourceOutputSchema3->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(sourceOutputSchema3->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(sourceOutputSchema3->hasFieldName("default_logical3$ts"));

    SchemaPtr joinOutSchema1 = joinOperators[0]->getOutputSchema();
    NES_DEBUG("expected join1= " << joinOperators[0]->getOutputSchema()->toString());
    EXPECT_TRUE(joinOutSchema1->fields.size() == 21);
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$start"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$end"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$key"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2$start"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2$end"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logicaldefault_logical2$key"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical$ts"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical2$f3"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical2$f4"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical2$ts"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3default_logical4$start"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3default_logical4$end"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3default_logical4$key"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical3$ts"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical4$f7"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical4$f8"));
    EXPECT_TRUE(joinOutSchema1->hasFieldName("default_logical4$ts"));

    SchemaPtr joinOutSchema2 = joinOperators[1]->getOutputSchema();
    NES_DEBUG("expected join2= " << joinOperators[1]->getOutputSchema()->toString());
    EXPECT_TRUE(joinOutSchema2->fields.size() == 9);
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logicaldefault_logical2$start"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logicaldefault_logical2$end"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logicaldefault_logical2$key"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical$ts"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical2$f3"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical2$f4"));
    EXPECT_TRUE(joinOutSchema2->hasFieldName("default_logical2$ts"));

    SchemaPtr joinOutSchema3 = joinOperators[2]->getOutputSchema();
    NES_DEBUG("expected join3= " << joinOperators[2]->getOutputSchema()->toString());
    EXPECT_TRUE(joinOutSchema3->fields.size() == 9);
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3default_logical4$start"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3default_logical4$end"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3default_logical4$key"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical3$ts"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical4$f7"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical4$f8"));
    EXPECT_TRUE(joinOutSchema3->hasFieldName("default_logical4$ts"));

    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected sinkOutputSchema= " << sinkOutputSchema->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 21);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2default_logical3default_logical4$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logicaldefault_logical2$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f1"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$f2"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical$ts"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical2$f3"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical2$f4"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical2$ts"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3default_logical4$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3default_logical4$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3default_logical4$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3$f5"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3$f6"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical3$ts"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical4$f7"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical4$f8"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("default_logical4$ts"));
}

/**
 * @brief In this test we infer the output schemas of multiple orWith Operators (equivalent to union)
 */
TEST_F(TypeInferencePhaseTest, inferOrwithQuery) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("QnV1", inputSchema);
    streamCatalog->addLogicalSource("QnV2", inputSchema);
    streamCatalog->addLogicalSource("QnV3", inputSchema);

    auto query = Query::from("QnV1")
                     .filter(Attribute("velocity") > 50)
                     .orWith(Query::from("QnV2")
                                 .filter(Attribute("quantity") > 5)
                                 .orWith(Query::from("QnV3").filter(Attribute("quantity") > 7)))
                     .sink(FileSinkDescriptor::create(""));

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    auto sink = resultPlan->getSinkOperators()[0];

    NES_INFO(sink->getOutputSchema()->toString());

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 4);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$quantity"));
}

/**
 * @brief In this test we infer the output schemas of multiple andWith Operators
 */
TEST_F(TypeInferencePhaseTest, inferAndwithQuery) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("QnV", inputSchema);
    streamCatalog->addLogicalSource("QnV1", inputSchema);
    streamCatalog->addLogicalSource("QnV2", inputSchema);

    auto query = Query::from("QnV")
                     .filter(Attribute("velocity") > 50)
                     .andWith(Query::from("QnV1").filter(Attribute("quantity") > 50))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)))
                     .andWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)))
                     .sink(FileSinkDescriptor::create(""));

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    auto sink = resultPlan->getSinkOperators()[0];

    NES_INFO(sink->getOutputSchema()->toString());

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 22);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$quantity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$quantity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$quantity"));
}

/**
 * @brief In this test we infer the output schemas of multiple seqWith Operators
 */
TEST_F(TypeInferencePhaseTest, inferMultiSeqwithQuery) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("QnV", inputSchema);
    streamCatalog->addLogicalSource("QnV1", inputSchema);
    streamCatalog->addLogicalSource("QnV2", inputSchema);

    auto query = Query::from("QnV")
                     .filter(Attribute("velocity") > 50)
                     .seqWith(Query::from("QnV1").filter(Attribute("quantity") > 50))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)))
                     .seqWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)))
                     .sink(FileSinkDescriptor::create(""));

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    auto sink = resultPlan->getSinkOperators()[0];

    NES_INFO(sink->getOutputSchema()->toString());

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 22);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1QnV2$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$quantity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$quantity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV2$quantity"));
}

/**
 * @brief In this test we infer the output schemas of a single seqWith Operators
 */
TEST_F(TypeInferencePhaseTest, inferSingleSeqwithQuery) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("QnV", inputSchema);
    streamCatalog->addLogicalSource("QnV1", inputSchema);
    streamCatalog->addLogicalSource("QnV2", inputSchema);

    auto query = Query::from("QnV")
                     .filter(Attribute("velocity") > 50)
                     .seqWith(Query::from("QnV1").filter(Attribute("quantity") > 50))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(10), Minutes(2)))
                     .sink(FileSinkDescriptor::create(""));

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(query.getQueryPlan());

    auto sink = resultPlan->getSinkOperators()[0];

    NES_INFO(sink->getOutputSchema()->toString());

    auto sinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = sinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << sinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 13);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$start"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$end"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnVQnV1$key"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV$quantity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$sensor_id"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$timestamp"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$velocity"));
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("QnV1$quantity"));
}

/**
 * @brief In this test we infer schema for a query with mapudf defined
 */
TEST_F(TypeInferencePhaseTest, inferTypeForQueryWithMapUDF) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("logicalSource", inputSchema);

    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(NullOutputSinkDescriptor::create());

    auto javaUdfDescriptor =
        Catalogs::UDF::JavaUdfDescriptorBuilder{}
            .setOutputSchema(std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createBoolean()))
            .build();
    auto mapUdfLogicalOperatorNode =
        std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor, Util::getNextOperatorId());

    auto descriptor = LogicalSourceDescriptor::create("logicalSource");
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    sinkOperator->addChild(mapUdfLogicalOperatorNode);
    mapUdfLogicalOperatorNode->addChild(sourceOperator);
    auto queryPlan = QueryPlan::create(sinkOperator);

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(queryPlan);

    auto actualSinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = actualSinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << actualSinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 1);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("logicalSource$outputAttribute"));
}

/**
 * @brief In this test we infer schema for a query with mapudf defined
 */
TEST_F(TypeInferencePhaseTest, inferTypeForQueryWithMapUDFAfterBinaryOperator) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("logicalSource1", inputSchema);
    streamCatalog->addLogicalSource("logicalSource2", inputSchema);

    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(NullOutputSinkDescriptor::create());

    auto javaUdfDescriptor =
        Catalogs::UDF::JavaUdfDescriptorBuilder{}
            .setOutputSchema(std::make_shared<Schema>()->addField("outputAttribute", DataTypeFactory::createBoolean()))
            .build();
    auto mapUdfLogicalOperatorNode =
        std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor, Util::getNextOperatorId());

    auto descriptor1 = LogicalSourceDescriptor::create("logicalSource1");
    auto sourceOperator1 = LogicalOperatorFactory::createSourceOperator(descriptor1);

    auto descriptor2 = LogicalSourceDescriptor::create("logicalSource2");
    auto sourceOperator2 = LogicalOperatorFactory::createSourceOperator(descriptor2);

    //Create union operator
    auto unionOperator = LogicalOperatorFactory::createUnionOperator();

    sinkOperator->addChild(mapUdfLogicalOperatorNode);
    mapUdfLogicalOperatorNode->addChild(unionOperator);
    unionOperator->addChild(sourceOperator1);
    unionOperator->addChild(sourceOperator2);
    auto queryPlan = QueryPlan::create(sinkOperator);

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(queryPlan);

    auto actualSinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = actualSinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << actualSinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 1);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("logicalSource1$outputAttribute"));
}

/**
 * @brief In this test we infer schema for a query with mapudf defined
 */
TEST_F(TypeInferencePhaseTest, inferTypeForQueryWithMapUDFBeforeBinaryOperator) {
    Catalogs::Source::SourceCatalogPtr streamCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    auto inputSchema = Schema::create()
                           ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                           ->addField(createField("timestamp", UINT64))
                           ->addField(createField("velocity", FLOAT32))
                           ->addField(createField("quantity", UINT64));

    streamCatalog->addLogicalSource("logicalSource1", inputSchema);
    streamCatalog->addLogicalSource("logicalSource2", inputSchema);

    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(NullOutputSinkDescriptor::create());

    auto javaUdfDescriptor1 =
        Catalogs::UDF::JavaUdfDescriptorBuilder{}
            .setOutputSchema(std::make_shared<Schema>()->addField("outputAttribute1", DataTypeFactory::createBoolean()))
            .build();
    auto mapUdfLogicalOperatorNode1 =
        std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor1, Util::getNextOperatorId());

    auto javaUdfDescriptor2 =
        Catalogs::UDF::JavaUdfDescriptorBuilder{}
            .setOutputSchema(std::make_shared<Schema>()->addField("outputAttribute2", DataTypeFactory::createBoolean()))
            .build();
    auto mapUdfLogicalOperatorNode2 =
        std::make_shared<MapJavaUdfLogicalOperatorNode>(javaUdfDescriptor1, Util::getNextOperatorId());

    auto descriptor1 = LogicalSourceDescriptor::create("logicalSource1");
    auto sourceOperator1 = LogicalOperatorFactory::createSourceOperator(descriptor1);

    auto descriptor2 = LogicalSourceDescriptor::create("logicalSource2");
    auto sourceOperator2 = LogicalOperatorFactory::createSourceOperator(descriptor2);

    auto unionOperator = LogicalOperatorFactory::createUnionOperator();

    //Build query plan
    sinkOperator->addChild(unionOperator);
    unionOperator->addChild(mapUdfLogicalOperatorNode1);
    unionOperator->addChild(mapUdfLogicalOperatorNode2);
    mapUdfLogicalOperatorNode1->addChild(sourceOperator1);
    mapUdfLogicalOperatorNode2->addChild(sourceOperator2);
    auto queryPlan = QueryPlan::create(sinkOperator);

    auto phase = Optimizer::TypeInferencePhase::create(streamCatalog, udfCatalog);
    auto resultPlan = phase->execute(queryPlan);

    auto actualSinkOperator = resultPlan->getOperatorByType<SinkLogicalOperatorNode>();
    SchemaPtr sinkOutputSchema = actualSinkOperator[0]->getOutputSchema();
    NES_DEBUG("expected = " << actualSinkOperator[0]->getOutputSchema()->toString());
    EXPECT_TRUE(sinkOutputSchema->fields.size() == 1);
    EXPECT_TRUE(sinkOutputSchema->hasFieldName("logicalSource1$outputAttribute1"));
}
}// namespace NES

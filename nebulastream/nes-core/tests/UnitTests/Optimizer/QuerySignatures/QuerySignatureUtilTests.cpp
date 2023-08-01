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
#include <API/Expressions/ArithmeticalExpressions.hpp>
#include <API/Expressions/Expressions.hpp>
#include <API/Expressions/LogicalExpressions.hpp>
#include <API/Schema.hpp>
#include <API/Windowing.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorFactory.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/TypeInferencePhaseContext.hpp>
#include <Optimizer/QuerySignatures/QuerySignature.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Optimizer/QuerySignatures/SignatureEqualityUtil.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/Watermark/EventTimeWatermarkStrategyDescriptor.hpp>
#include <Windowing/Watermark/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <z3++.h>

using namespace NES;

class QuerySignatureUtilTests : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    SchemaPtr schema;
    std::shared_ptr<QueryParsingService> queryParsingService;
    std::shared_ptr<Compiler::JITCompiler> jitCompiler;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UdfCatalogPtr udfCatalog;
    std::shared_ptr<Optimizer::TypeInferencePhaseContext> typeInferencePhaseContext;

    /* Will be called before all tests in this class are started. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QuerySignatureUtilTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QuerySignatureUtilTests test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::TestWithErrorHandling<testing::Test>::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();
        typeInferencePhaseContext = std::make_shared<Optimizer::TypeInferencePhaseContext>(
            Optimizer::TypeInferencePhaseContext(sourceCatalog, udfCatalog));
        schema = Schema::create()->addField("test$id", BasicType::UINT32)->addField("test$value", BasicType::UINT64);
    }
};

TEST_F(QuerySignatureUtilTests, testFiltersWithExactPredicates) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    ExpressionNodePtr predicate = Attribute("value") == 40;
    predicate->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithEqualPredicates) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);
    ExpressionNodePtr predicate2 = 40 == Attribute("value");
    predicate2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithMultipleExactPredicates) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithMultipleEqualPredicates1) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 40;
    predicate2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithMultipleEqualPredicates2) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 + 40 && Attribute("id") >= 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 && Attribute("value") == 80;
    predicate2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);
    ExpressionNodePtr predicate2 = Attribute("id") == 40;
    predicate2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testFiltersWithMultipleDifferentPredicates) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();

    //Define Predicate
    ExpressionNodePtr predicate1 = Attribute("value") == 40 && Attribute("id") >= 40;
    predicate1->inferStamp(*typeInferencePhaseContext, schema);
    ExpressionNodePtr predicate2 = Attribute("id") >= 40 or Attribute("value") == 40;
    predicate2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createFilterOperator(predicate1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();
    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createFilterOperator(predicate2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testMapWithExactExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression = Attribute("value") = 40;
    expression->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testMapWithDifferentExpression) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    FieldAssignmentExpressionNodePtr expression2 = Attribute("id") = 40;

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testMultipleMapsWithDifferentOrder) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("id") = 40;
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = Attribute("id") + Attribute("value");

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create map
    LogicalOperatorNodePtr logicalOperator11 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator11->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator12 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator12->addChild(logicalOperator11);
    logicalOperator12->inferSchema(*typeInferencePhaseContext);
    logicalOperator12->inferZ3Signature(context);
    auto sig1 = logicalOperator12->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator21 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator21->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator22 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator22->addChild(logicalOperator21);
    logicalOperator22->inferSchema(*typeInferencePhaseContext);
    logicalOperator22->inferZ3Signature(context);
    auto sig2 = logicalOperator22->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testMultipleMapsWithSameOrder) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("id") = 40;
    expression1->inferStamp(*typeInferencePhaseContext, schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = Attribute("id") + Attribute("value");
    expression2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create map
    LogicalOperatorNodePtr logicalOperator11 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator11->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator12 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator12->addChild(logicalOperator11);
    logicalOperator12->inferSchema(*typeInferencePhaseContext);
    logicalOperator12->inferZ3Signature(context);
    auto sig1 = logicalOperator12->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator21 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator21->addChild(sourceOperator);
    LogicalOperatorNodePtr logicalOperator22 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator22->addChild(logicalOperator21);
    logicalOperator22->inferSchema(*typeInferencePhaseContext);
    logicalOperator22->inferZ3Signature(context);
    auto sig2 = logicalOperator22->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testMapWithDifferentExpressionOnSameField) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define expression
    FieldAssignmentExpressionNodePtr expression1 = Attribute("value") = 40;
    expression1->inferStamp(*typeInferencePhaseContext, schema);
    FieldAssignmentExpressionNodePtr expression2 = Attribute("value") = 50;
    expression2->inferStamp(*typeInferencePhaseContext, schema);

    //Create Source
    auto descriptor = LogicalSourceDescriptor::create("car");
    descriptor->setSchema(schema);
    LogicalOperatorNodePtr sourceOperator = LogicalOperatorFactory::createSourceOperator(descriptor);

    //Create Filters
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createMapOperator(expression1);
    logicalOperator1->addChild(sourceOperator);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createMapOperator(expression2);
    logicalOperator2->addChild(sourceOperator);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSourceWithSameSourceName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor = LogicalSourceDescriptor::create("Car");
    sourceDescriptor->setSchema(schema);

    //Create source operator
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSourceWithDifferentSourceName) {

    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Predicate
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Truck");
    sourceDescriptor2->setSchema(schema);

    //Create source
    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    logicalOperator1->inferSchema(*typeInferencePhaseContext);
    logicalOperator1->inferZ3Signature(context);
    auto sig1 = logicalOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    logicalOperator2->inferSchema(*typeInferencePhaseContext);
    logicalOperator2->inferZ3Signature(context);
    auto sig2 = logicalOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForProjectOperators) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto projectionOperator1 = LogicalOperatorFactory::createProjectionOperator(
        {FieldAccessExpressionNode::create("id"), FieldAccessExpressionNode::create("value")});
    auto projectionOperator2 = LogicalOperatorFactory::createProjectionOperator(
        {FieldAccessExpressionNode::create("id"), FieldAccessExpressionNode::create("value")});

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    projectionOperator1->addChild(logicalOperator1);
    projectionOperator1->inferSchema(*typeInferencePhaseContext);
    projectionOperator1->inferZ3Signature(context);
    auto sig1 = projectionOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    projectionOperator2->addChild(logicalOperator2);
    projectionOperator2->inferSchema(*typeInferencePhaseContext);
    projectionOperator2->inferZ3Signature(context);
    auto sig2 = projectionOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForSameProjectOperatorsButDifferentSources) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Truck");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto projectionOperator1 = LogicalOperatorFactory::createProjectionOperator(
        {FieldAccessExpressionNode::create("id"), FieldAccessExpressionNode::create("value")});
    auto projectionOperator2 = LogicalOperatorFactory::createProjectionOperator(
        {FieldAccessExpressionNode::create("id"), FieldAccessExpressionNode::create("value")});

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    projectionOperator1->addChild(logicalOperator1);
    projectionOperator1->inferSchema(*typeInferencePhaseContext);
    projectionOperator1->inferZ3Signature(context);
    auto sig1 = projectionOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    projectionOperator2->addChild(logicalOperator2);
    projectionOperator2->inferSchema(*typeInferencePhaseContext);
    projectionOperator2->inferZ3Signature(context);
    auto sig2 = projectionOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForDifferenProjectOperators) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto projectionOperator1 = LogicalOperatorFactory::createProjectionOperator(
        {FieldAccessExpressionNode::create("id"), FieldAccessExpressionNode::create("value")});
    auto projectionOperator2 = LogicalOperatorFactory::createProjectionOperator({FieldAccessExpressionNode::create("id")});

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    projectionOperator1->addChild(logicalOperator1);
    projectionOperator1->inferSchema(*typeInferencePhaseContext);
    projectionOperator1->inferZ3Signature(context);
    auto sig1 = projectionOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    projectionOperator2->addChild(logicalOperator2);
    projectionOperator2->inferSchema(*typeInferencePhaseContext);
    projectionOperator2->inferZ3Signature(context);
    auto sig2 = projectionOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForWatermarkAssignerOperator) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto watermarkOperator1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));
    auto watermarkOperator2 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    watermarkOperator1->addChild(logicalOperator1);
    watermarkOperator1->inferSchema(*typeInferencePhaseContext);
    watermarkOperator1->inferZ3Signature(context);
    auto sig1 = watermarkOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    watermarkOperator2->addChild(logicalOperator2);
    watermarkOperator2->inferSchema(*typeInferencePhaseContext);
    watermarkOperator2->inferZ3Signature(context);
    auto sig2 = watermarkOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForIngestionTimeWatermarkAssignerOperator) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto watermarkOperator1 =
        LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());
    auto watermarkOperator2 =
        LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    watermarkOperator1->addChild(logicalOperator1);
    watermarkOperator1->inferSchema(*typeInferencePhaseContext);
    watermarkOperator1->inferZ3Signature(context);
    auto sig1 = watermarkOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    watermarkOperator2->addChild(logicalOperator2);
    watermarkOperator2->inferSchema(*typeInferencePhaseContext);
    watermarkOperator2->inferZ3Signature(context);
    auto sig2 = watermarkOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_TRUE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForDifferentWatermarkAssignerOperator) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto watermarkOperator1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));
    auto watermarkOperator2 =
        LogicalOperatorFactory::createWatermarkAssignerOperator(Windowing::IngestionTimeWatermarkStrategyDescriptor::create());

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    watermarkOperator1->addChild(logicalOperator1);
    watermarkOperator1->inferSchema(*typeInferencePhaseContext);
    watermarkOperator1->inferZ3Signature(context);
    auto sig1 = watermarkOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    watermarkOperator2->addChild(logicalOperator2);
    watermarkOperator2->inferSchema(*typeInferencePhaseContext);
    watermarkOperator2->inferZ3Signature(context);
    auto sig2 = watermarkOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForWatermarkAssignerOperatorWithDifferentLateness) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto watermarkOperator1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));
    auto watermarkOperator2 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(9),
                                                                NES::API::Milliseconds()));

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    watermarkOperator1->addChild(logicalOperator1);
    watermarkOperator1->inferSchema(*typeInferencePhaseContext);
    watermarkOperator1->inferZ3Signature(context);
    auto sig1 = watermarkOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    watermarkOperator2->addChild(logicalOperator2);
    watermarkOperator2->inferSchema(*typeInferencePhaseContext);
    watermarkOperator2->inferZ3Signature(context);
    auto sig2 = watermarkOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}

TEST_F(QuerySignatureUtilTests, testSignatureComputationForWatermarkAssignerOperatorWithDifferentField) {
    std::shared_ptr<z3::context> context = std::make_shared<z3::context>();
    //Define Sources
    auto sourceDescriptor1 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor1->setSchema(schema);
    auto sourceDescriptor2 = LogicalSourceDescriptor::create("Car");
    sourceDescriptor2->setSchema(schema);

    //Create projection operator
    auto watermarkOperator1 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("id"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));
    auto watermarkOperator2 = LogicalOperatorFactory::createWatermarkAssignerOperator(
        Windowing::EventTimeWatermarkStrategyDescriptor::create(Attribute("value"),
                                                                NES::API::Milliseconds(10),
                                                                NES::API::Milliseconds()));

    LogicalOperatorNodePtr logicalOperator1 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor1);
    watermarkOperator1->addChild(logicalOperator1);
    watermarkOperator1->inferSchema(*typeInferencePhaseContext);
    watermarkOperator1->inferZ3Signature(context);
    auto sig1 = watermarkOperator1->getZ3Signature();

    LogicalOperatorNodePtr logicalOperator2 = LogicalOperatorFactory::createSourceOperator(sourceDescriptor2);
    watermarkOperator2->addChild(logicalOperator2);
    watermarkOperator2->inferSchema(*typeInferencePhaseContext);
    watermarkOperator2->inferZ3Signature(context);
    auto sig2 = watermarkOperator2->getZ3Signature();

    //Assert
    auto signatureEqualityUtil = Optimizer::SignatureEqualityUtil::create(context);
    EXPECT_FALSE(signatureEqualityUtil->checkEquality(sig1, sig2));
}
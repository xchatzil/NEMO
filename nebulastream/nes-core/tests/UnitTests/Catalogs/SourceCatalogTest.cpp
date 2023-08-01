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

#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Exceptions/MapEntryNotFoundException.hpp>
#include <NesBaseTest.hpp>
#include <Services/QueryParsingService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <iostream>

#include <Util/Logger/Logger.hpp>

using namespace std;
using namespace NES;
using namespace Configurations;
std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
const std::string defaultLogicalSourceName = "default_logical";

/* - nesTopologyManager ---------------------------------------------------- */
class SourceCatalogTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    }
};

TEST_F(SourceCatalogTest, testAddGetLogSource) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    sourceCatalog->addLogicalSource("test_stream", Schema::create());
    SchemaPtr sPtr = sourceCatalog->getSchemaForLogicalSource("test_stream");
    EXPECT_NE(sPtr, nullptr);

    map<std::string, SchemaPtr> allLogicalSource = sourceCatalog->getAllLogicalSource();
    string exp = "id:INTEGER value:INTEGER ";
    EXPECT_EQ(allLogicalSource.size(), 3U);

    SchemaPtr testSchema = allLogicalSource["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalSource["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(SourceCatalogTest, testAddRemoveLogSource) {
    sourceCatalog->addLogicalSource("test_stream", Schema::create());

    EXPECT_TRUE(sourceCatalog->removeLogicalSource("test_stream"));

    EXPECT_THROW(sourceCatalog->getSchemaForLogicalSource("test_stream"), MapEntryNotFoundException);

    string exp = "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream "
                 "name=test_stream schema:\n\n";

    map<std::string, SchemaPtr> allLogicalSource = sourceCatalog->getAllLogicalSource();

    EXPECT_NE(1ul, allLogicalSource.size());
    EXPECT_FALSE(sourceCatalog->removeLogicalSource("test_stream22"));
}

TEST_F(SourceCatalogTest, testGetNotExistingKey) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    EXPECT_THROW(sourceCatalog->getSchemaForLogicalSource("test_stream22"), MapEntryNotFoundException);
}

TEST_F(SourceCatalogTest, testAddGetPhysicalSource) {

    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
}

//TODO: add test for a second physical source add

TEST_F(SourceCatalogTest, testAddRemovePhysicalSource) {
    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    EXPECT_TRUE(sourceCatalog->removePhysicalSource(physicalSource->getLogicalSourceName(),
                                                    physicalSource->getPhysicalSourceName(),
                                                    physicalNode->getId()));
    NES_INFO(sourceCatalog->getPhysicalSourceAndSchemaAsString());
}

TEST_F(SourceCatalogTest, testAddPhysicalForNotExistingLogicalSource) {
    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
}
//new from service
TEST_F(SourceCatalogTest, testGetAllLogicalSource) {

    const map<std::string, std::string>& allLogicalSource = sourceCatalog->getAllLogicalSourceAsString();
    EXPECT_EQ(allLogicalSource.size(), 2U);
    for (auto const& [key, value] : allLogicalSource) {
        bool cmp = key != defaultLogicalSourceName && key != "exdra" && key != "iris";
        EXPECT_EQ(cmp, false);
    }
}

TEST_F(SourceCatalogTest, testAddLogicalSourceFromString) {
    sourceCatalog->addLogicalSource("test", testSchema);
    const map<std::string, std::string>& allLogicalSource = sourceCatalog->getAllLogicalSourceAsString();
    EXPECT_EQ(allLogicalSource.size(), 3U);
}

TEST_F(SourceCatalogTest, testGetPhysicalSourceForLogicalSource) {
    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    sourceCatalog->addLogicalSource(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sce =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(sourceCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    const vector<Catalogs::Source::SourceCatalogEntryPtr>& allPhysicalSources =
        sourceCatalog->getPhysicalSources(logicalSource->getLogicalSourceName());
    EXPECT_EQ(allPhysicalSources.size(), 1U);
}

TEST_F(SourceCatalogTest, testDeleteLogicalSource) {
    bool success = sourceCatalog->removeLogicalSource(defaultLogicalSourceName);
    EXPECT_TRUE(success);
}

TEST_F(SourceCatalogTest, testUpdateLogicalSourceWithInvalidSourceName) {
    std::string logicalSourceName = "test";
    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    bool success = sourceCatalog->updatedLogicalSource(logicalSourceName, newSchema);
    EXPECT_FALSE(success);
}

TEST_F(SourceCatalogTest, testUpdateLogicalSource) {
    std::string logicalSourceName = "test";
    bool success = sourceCatalog->addLogicalSource(logicalSourceName, testSchema);
    EXPECT_TRUE(success);

    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    success = sourceCatalog->updatedLogicalSource(logicalSourceName, newSchema);
    EXPECT_TRUE(success);
}
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

#include <API/Query.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

using namespace NES;

class GlobalExecutionPlanTest : public Testing::TestWithErrorHandling<testing::Test> {

  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() {
        Logger::setupLogging("GlobalExecutionPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GlobalExecutionPlanTest test case.");
    }
};

/**
 * @brief This test is for validating different behaviour for an empty global execution plan
 */
TEST_F(GlobalExecutionPlanTest, testCreateEmptyGlobalExecutionPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    std::string actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan;

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node without any plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithoutAnyPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    std::string actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId()) + ")\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with one plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithOnePlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("car");
    auto query = Query::from("truck").unionWith(subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId = PlanIdGenerator::getNextQuerySubPlanId();
    plan->setQueryId(queryId);
    plan->setQuerySubPlanId(querySubPlanId);
    executionNode->addNewQuerySubPlan(queryId, plan);

    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string actualPlan = globalExecutionPlan->getAsString();

    NES_INFO("GlobalExecutionPlanTest: Actual plan: \n" + actualPlan);

    NES_INFO("GlobalExecutionPlanTest: queryPlan.toString(): \n" + plan->toString());

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId) + ", querySubPlanId:" + std::to_string(querySubPlanId)
        + ")\n"
          "|  "
        + plan->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|      "
        + plan->getRootOperators()[0]->getChildren()[0]->getChildren()[0]->toString()
        + "\n"
          "|      "
        + plan->getRootOperators()[0]->getChildren()[0]->getChildren()[1]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = PlanIdGenerator::getNextQuerySubPlanId();
    plan1->setQueryId(queryId);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode->addNewQuerySubPlan(queryId, plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    QuerySubPlanId querySubPlanId2 = PlanIdGenerator::getNextQuerySubPlanId();
    plan2->setQueryId(queryId);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode->addNewQuerySubPlan(queryId, plan2);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(query1.getQueryPlan()->getQueryId()) + ", querySubPlanId:" + std::to_string(querySubPlanId1)
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(query2.getQueryPlan()->getQueryId()) + ", querySubPlanId:" + std::to_string(querySubPlanId2)
        + ")\n"
          "|  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan2->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan for different queryIdAndCatalogEntryMapping
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlanForDifferentqueries) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = PlanIdGenerator::getNextQuerySubPlanId();
    plan1->setQueryId(queryId1);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode->addNewQuerySubPlan(queryId1, plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    QuerySubPlanId querySubPlanId2 = PlanIdGenerator::getNextQuerySubPlanId();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    plan2->setQueryId(queryId2);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode->addNewQuerySubPlan(queryId2, plan2);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId1) + ", querySubPlanId:" + std::to_string(querySubPlanId1)
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId2) + ", querySubPlanId:" + std::to_string(querySubPlanId2)
        + ")\n"
          "|  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan2->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with 4 plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithFourPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    //query sub plans for query 1
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor11 = PrintSinkDescriptor::create();
    auto query11 = Query::from("default_logical").sink(printSinkDescriptor11);
    auto plan11 = query11.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId11 = PlanIdGenerator::getNextQuerySubPlanId();
    plan11->setQueryId(queryId1);
    plan11->setQuerySubPlanId(querySubPlanId11);
    executionNode->addNewQuerySubPlan(queryId1, plan11);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor12 = PrintSinkDescriptor::create();
    auto query12 = Query::from("default_logical").sink(printSinkDescriptor12);
    auto plan12 = query12.getQueryPlan();
    QuerySubPlanId querySubPlanId12 = PlanIdGenerator::getNextQuerySubPlanId();
    plan12->setQueryId(queryId1);
    plan12->setQuerySubPlanId(querySubPlanId12);
    executionNode->addNewQuerySubPlan(queryId1, plan12);

    //query sub plans for query 2
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor21 = PrintSinkDescriptor::create();
    auto query21 = Query::from("default_logical").sink(printSinkDescriptor21);
    auto plan21 = query21.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId21 = PlanIdGenerator::getNextQuerySubPlanId();
    plan21->setQueryId(queryId2);
    plan21->setQuerySubPlanId(querySubPlanId21);
    executionNode->addNewQuerySubPlan(queryId2, plan21);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor22 = PrintSinkDescriptor::create();
    auto query22 = Query::from("default_logical").sink(printSinkDescriptor22);
    auto plan22 = query22.getQueryPlan();
    QuerySubPlanId querySubPlanId22 = PlanIdGenerator::getNextQuerySubPlanId();
    plan22->setQueryId(queryId2);
    plan22->setQuerySubPlanId(querySubPlanId22);
    executionNode->addNewQuerySubPlan(queryId2, plan22);

    globalExecutionPlan->addExecutionNode(executionNode);

    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId1) + ", querySubPlanId:" + std::to_string(querySubPlanId11)
        + ")\n"
          "|  "
        + plan11->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan11->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId1) + ", querySubPlanId:" + std::to_string(querySubPlanId12)
        + ")\n"
          "|  "
        + plan12->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan12->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId2) + ", querySubPlanId:" + std::to_string(querySubPlanId21)
        + ")\n"
          "|  "
        + plan21->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan21->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId2) + ", querySubPlanId:" + std::to_string(querySubPlanId22)
        + ")\n"
          "|  "
        + plan22->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan22->getRootOperators()[0]->getChildren()[0]->toString() + "\n";
    NES_INFO("Actual query plan \n" << actualPlan);

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with two execution nodes with one plan each
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithTwoExecutionNodesEachWithOnePlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    uint64_t node1Id = 1;
    TopologyNodePtr topologyNode1 = TopologyNode::create(node1Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode1 = ExecutionNode::createExecutionNode(topologyNode1);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = PlanIdGenerator::getNextQuerySubPlanId();
    plan1->setQueryId(queryId1);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode1->addNewQuerySubPlan(queryId1, plan1);

    //create execution node
    uint64_t node2Id = 2;
    TopologyNodePtr topologyNode2 = TopologyNode::create(node2Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode2 = ExecutionNode::createExecutionNode(topologyNode2);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId2 = PlanIdGenerator::getNextQuerySubPlanId();
    plan2->setQueryId(queryId2);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode2->addNewQuerySubPlan(queryId2, plan2);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);

    globalExecutionPlan->addExecutionNodeAsParentTo(executionNode1->getId(), executionNode2);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode2);

    const std::string& actualPlan = globalExecutionPlan->getAsString();

    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode2->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode2->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId2) + ", querySubPlanId:" + std::to_string(querySubPlanId2)
        + ")\n"
          "|  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan2->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + std::to_string(executionNode1->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode1->getTopologyNode()->getId())
        + ")\n"
          "|  | QuerySubPlan(queryId:"
        + std::to_string(queryId1) + ", querySubPlanId:" + std::to_string(querySubPlanId1)
        + ")\n"
          "|  |  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |    "
        + plan1->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}
/**
 * @brief This test is for validating behaviour for a global execution plan with nested execution node with one plan for different queryIdAndCatalogEntryMapping
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithTwoExecutionNodesEachWithOnePlanToString) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node 1
    uint64_t node1Id = 1;
    TopologyNodePtr topologyNode1 = TopologyNode::create(node1Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode1 = ExecutionNode::createExecutionNode(topologyNode1);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = PlanIdGenerator::getNextQuerySubPlanId();
    plan1->setQueryId(queryId1);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode1->addNewQuerySubPlan(queryId1, plan1);

    //create execution node 2
    uint64_t node2Id = 2;
    TopologyNodePtr topologyNode2 = TopologyNode::create(node2Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode2 = ExecutionNode::createExecutionNode(topologyNode2);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId2 = PlanIdGenerator::getNextQuerySubPlanId();
    plan2->setQueryId(queryId2);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode2->addNewQuerySubPlan(queryId2, plan2);

    //create execution node 3
    uint64_t node3Id = 3;
    TopologyNodePtr topologyNode3 = TopologyNode::create(node3Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode3 = ExecutionNode::createExecutionNode(topologyNode3);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query3 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan3 = query3.getQueryPlan();
    QueryId queryId3 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId3 = PlanIdGenerator::getNextQuerySubPlanId();
    plan3->setQueryId(queryId3);
    plan3->setQuerySubPlanId(querySubPlanId3);
    executionNode3->addNewQuerySubPlan(queryId3, plan3);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);
    globalExecutionPlan->addExecutionNode(executionNode3);

    //create execution node 4
    uint64_t node4Id = 4;
    TopologyNodePtr topologyNode4 = TopologyNode::create(node4Id, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode4 = ExecutionNode::createExecutionNode(topologyNode4);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query4 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan4 = query4.getQueryPlan();
    QueryId queryId4 = PlanIdGenerator::getNextQueryId();
    QuerySubPlanId querySubPlanId4 = PlanIdGenerator::getNextQuerySubPlanId();
    plan4->setQueryId(queryId4);
    plan4->setQuerySubPlanId(querySubPlanId4);
    executionNode4->addNewQuerySubPlan(queryId4, plan4);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);
    globalExecutionPlan->addExecutionNode(executionNode3);
    globalExecutionPlan->addExecutionNode(executionNode4);

    globalExecutionPlan->addExecutionNodeAsParentTo(executionNode3->getId(), executionNode4);
    globalExecutionPlan->addExecutionNodeAsParentTo(executionNode2->getId(), executionNode3);
    globalExecutionPlan->addExecutionNodeAsParentTo(executionNode1->getId(), executionNode4);

    globalExecutionPlan->addExecutionNodeAsRoot(executionNode4);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n" << actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode4->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode4->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(queryId:"
        + std::to_string(queryId4) + ", querySubPlanId:" + std::to_string(querySubPlanId4)
        + ")\n"
          "|  "
        + plan4->getRootOperators()[0]->toString()
        + "\n"
          "|    "
        + plan4->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + std::to_string(executionNode3->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode3->getTopologyNode()->getId())
        + ")\n"
          "|  | QuerySubPlan(queryId:"
        + std::to_string(queryId3) + ", querySubPlanId:" + std::to_string(querySubPlanId3)
        + ")\n"
          "|  |  "
        + plan3->getRootOperators()[0]->toString()
        + "\n"
          "|  |    "
        + plan3->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |--ExecutionNode(id:"
        + std::to_string(executionNode2->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode2->getTopologyNode()->getId())
        + ")\n"
          "|  |  | QuerySubPlan(queryId:"
        + std::to_string(queryId2) + ", querySubPlanId:" + std::to_string(querySubPlanId2)
        + ")\n"
          "|  |  |  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |    "
        + plan2->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + std::to_string(executionNode1->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode1->getTopologyNode()->getId())
        + ")\n"
          "|  | QuerySubPlan(queryId:"
        + std::to_string(queryId1) + ", querySubPlanId:" + std::to_string(querySubPlanId1)
        + ")\n"
          "|  |  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |    "
        + plan1->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}
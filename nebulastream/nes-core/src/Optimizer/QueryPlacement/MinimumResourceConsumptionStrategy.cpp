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
#include <Catalogs/SourceCatalog.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/Operator.hpp>
#include <Optimizer/ExecutionNode.hpp>
#include <Optimizer/NESExecutionPlan.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/MinimumResourceConsumptionStrategy.hpp>
#include <Optimizer/Utils/PathFinder.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/CodeGenerator/TranslateToLegacyExpression.hpp>
#include <Topology/NESTopologyPlan.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

MinimumResourceConsumptionStrategy::MinimumResourceConsumptionStrategy(NESTopologyPlanPtr nesTopologyPlan)
    : BasePlacementStrategy(nesTopologyPlan) {}

NESExecutionPlanPtr
MinimumResourceConsumptionStrategy::initializeExecutionPlan(QueryPlanPtr queryPlan,
                                                            NESTopologyPlanPtr nesTopologyPlan,
                                                            Catalogs::Source::SourceCatalogPtr sourceCatalog) {
    this->nesTopologyPlan = nesTopologyPlan;
    const SourceLogicalOperatorNodePtr sourceOperator = queryPlan->getSourceOperators()[0];

    // FIXME: current implementation assumes that we have only one source and therefore only one source operator.
    const string sourceName = queryPlan->getSourceName();

    if (!sourceOperator) {
        NES_THROW_RUNTIME_ERROR("MinimumResourceConsumption: Unable to find the source operator.");
    }

    const vector<NESTopologyEntryPtr> sourceNodes = sourceCatalog->getSourceNodesForLogicalSource(sourceName);

    if (sourceNodes.empty()) {
        NES_THROW_RUNTIME_ERROR("MinimumResourceConsumption: Unable to find the target source: " + sourceName);
    }

    NESExecutionPlanPtr nesExecutionPlanPtr = std::make_shared<NESExecutionPlan>();
    const NESTopologyGraphPtr nesTopologyGraphPtr = nesTopologyPlan->getNESTopologyGraph();

    NES_INFO("MinimumResourceConsumption: Placing operators on the nes topology.");
    placeOperators(nesExecutionPlanPtr, nesTopologyGraphPtr, sourceOperator, sourceNodes);

    NESTopologyEntryPtr rootNode = nesTopologyGraphPtr->getRoot();

    NES_DEBUG("MinimumResourceConsumption: Find the path used for performing the placement based on the strategy type");
    vector<NESTopologyEntryPtr> candidateNodes = getCandidateNodesForFwdOperatorPlacement(sourceNodes, rootNode);

    NES_INFO("MinimumResourceConsumption: Adding forward operators.");
    addSystemGeneratedOperators(candidateNodes, nesExecutionPlanPtr);

    NES_INFO("MinimumResourceConsumption: Generating complete execution Graph.");
    fillExecutionGraphWithTopologyInformation(nesExecutionPlanPtr);

    //FIXME: We are assuming that throughout the pipeline the schema would not change.
    SchemaPtr schema = sourceOperator->getSourceDescriptor()->getSchema();
    addSystemGeneratedSourceSinkOperators(schema, nesExecutionPlanPtr);

    return nesExecutionPlanPtr;
}

vector<NESTopologyEntryPtr>
MinimumResourceConsumptionStrategy::getCandidateNodesForFwdOperatorPlacement(const vector<NESTopologyEntryPtr>& sourceNodes,
                                                                             const NES::NESTopologyEntryPtr rootNode) const {

    auto pathMap = pathFinder->findUniquePathBetween(sourceNodes, rootNode);
    vector<NESTopologyEntryPtr> candidateNodes;
    for (auto [key, value] : pathMap) {
        candidateNodes.insert(candidateNodes.end(), value.begin(), value.end());
    }

    return candidateNodes;
}

void MinimumResourceConsumptionStrategy::placeOperators(NESExecutionPlanPtr executionPlanPtr,
                                                        NESTopologyGraphPtr nesTopologyGraphPtr,
                                                        LogicalOperatorNodePtr sourceOperator,
                                                        vector<NESTopologyEntryPtr> sourceNodes) {

    TranslateToLegacyPlanPhasePtr translator = TranslateToLegacyPlanPhase::create();
    const NESTopologyEntryPtr sinkNode = nesTopologyGraphPtr->getRoot();

    auto pathMap = pathFinder->findUniquePathBetween(sourceNodes, sinkNode);

    //Prepare list of ordered common nodes
    vector<vector<NESTopologyEntryPtr>> listOfPaths;
    transform(pathMap.begin(), pathMap.end(), back_inserter(listOfPaths), [](const auto pair) {
        return pair.second;
    });

    vector<NESTopologyEntryPtr> commonPath;

    for (uint64_t i = 0; i < listOfPaths.size(); i++) {
        vector<NESTopologyEntryPtr> path_i = listOfPaths[i];

        for (NESTopologyEntryPtr node_i : path_i) {
            bool nodeOccursInAllPaths = false;
            for (uint64_t j = i; j < listOfPaths.size(); j++) {
                if (i == j) {
                    continue;
                }

                vector<NESTopologyEntryPtr> path_j = listOfPaths[j];
                const auto itr = find_if(path_j.begin(), path_j.end(), [node_i](NESTopologyEntryPtr node_j) {
                    return node_i->getId() == node_j->getId();
                });

                if (itr != path_j.end()) {
                    nodeOccursInAllPaths = true;
                } else {
                    nodeOccursInAllPaths = false;
                    break;
                }
            }

            if (nodeOccursInAllPaths) {
                commonPath.push_back(node_i);
            }
        }
    }

    NES_DEBUG("MinimumResourceConsumption: Transforming New Operator into legacy operator");
    OperatorPtr legacySourceOperator = translator->transform(sourceOperator);

    for (NESTopologyEntryPtr sourceNode : sourceNodes) {

        NES_DEBUG("MinimumResourceConsumption: Create new execution node for source operator.");
        stringstream operatorName;
        operatorName << sourceOperator->toString() << "(OP-" << std::to_string(sourceOperator->getId()) << ")";

        const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                        to_string(sourceNode->getId()),
                                                                                        sourceNode,
                                                                                        legacySourceOperator->copy());
        newExecutionNode->addOperatorId(sourceOperator->getId());
        sourceNode->reduceCpuCapacity(1);
    }

    LogicalOperatorNodePtr targetOperator = sourceOperator->getParents()[0]->as<LogicalOperatorNode>();

    while (targetOperator && targetOperator->instanceOf<SinkLogicalOperatorNode>()) {
        NESTopologyEntryPtr node = nullptr;
        for (NESTopologyEntryPtr commonNode : commonPath) {
            if (commonNode->getRemainingCpuCapacity() > 0) {
                node = commonNode;
                break;
            }
        }

        if (!node) {
            NES_ERROR("MinimumResourceConsumption: Can not schedule the operator. No free resource available capacity is="
                      << sinkNode->getRemainingCpuCapacity());
            throw std::runtime_error("Can not schedule the operator. No free resource available.");
        }

        NES_DEBUG("MinimumResourceConsumption: suitable placement for operator " << targetOperator->toString() << " is "
                                                                                 << node->toString());

        NES_DEBUG("MinimumResourceConsumption: Transforming New Operator into legacy operator");
        OperatorPtr legacyOperator = translator->transform(targetOperator);

        // If the selected nes node was already used by another operator for placement then do not create a
        // new execution node rather add operator to existing node.
        if (executionPlanPtr->hasVertex(node->getId())) {

            NES_DEBUG("MinimumResourceConsumption: node " << node->toString() << " was already used by other deployment");

            const ExecutionNodePtr existingExecutionNode = executionPlanPtr->getExecutionNode(node->getId());

            stringstream operatorName;
            operatorName << existingExecutionNode->getOperatorName() << "=>" << targetOperator->toString() << "(OP-"
                         << std::to_string(targetOperator->getId()) << ")";
            existingExecutionNode->addOperator(legacyOperator->copy());
            existingExecutionNode->setOperatorName(operatorName.str());
            existingExecutionNode->addOperatorId(targetOperator->getId());
        } else {

            NES_DEBUG("MinimumResourceConsumption: create new execution node " << node->toString());

            stringstream operatorName;
            operatorName << targetOperator->toString() << "(OP-" << std::to_string(targetOperator->getId()) << ")";

            // Create a new execution node
            const ExecutionNodePtr newExecutionNode =
                executionPlanPtr->createExecutionNode(operatorName.str(), to_string(node->getId()), node, legacyOperator->copy());
            newExecutionNode->addOperatorId(targetOperator->getId());
        }

        // Reduce the processing capacity by 1
        // FIXME: Bring some logic here where the cpu capacity is reduced based on operator workload
        node->reduceCpuCapacity(1);

        if (!targetOperator->getParents().empty()) {
            targetOperator = targetOperator->getParents()[0]->as<LogicalOperatorNode>();
        }
    }

    if (sinkNode->getRemainingCpuCapacity() > 0) {
        NES_DEBUG("MinimumResourceConsumption: Transforming New Operator into legacy operator");
        OperatorPtr legacyOperator = translator->transform(targetOperator);
        if (executionPlanPtr->hasVertex(sinkNode->getId())) {

            NES_DEBUG("MinimumResourceConsumption: node " << sinkNode->toString() << " was already used by other deployment");

            const ExecutionNodePtr existingExecutionNode = executionPlanPtr->getExecutionNode(sinkNode->getId());

            stringstream operatorName;
            operatorName << existingExecutionNode->getOperatorName() << "=>" << targetOperator->toString() << "(OP-"
                         << std::to_string(targetOperator->getId()) << ")";
            existingExecutionNode->addOperator(legacyOperator->copy());
            existingExecutionNode->setOperatorName(operatorName.str());
            existingExecutionNode->addOperatorId(targetOperator->getId());
        } else {

            NES_DEBUG("MinimumResourceConsumption: create new execution node " << sinkNode->toString() << " with sink operator");

            stringstream operatorName;
            operatorName << targetOperator->toString() << "(OP-" << std::to_string(targetOperator->getId()) << ")";

            // Create a new execution node
            const ExecutionNodePtr newExecutionNode = executionPlanPtr->createExecutionNode(operatorName.str(),
                                                                                            to_string(sinkNode->getId()),
                                                                                            sinkNode,
                                                                                            legacyOperator->copy());
            newExecutionNode->addOperatorId(targetOperator->getId());
        }
        sinkNode->reduceCpuCapacity(1);
    } else {
        NES_ERROR("MinimumResourceConsumption: Can not schedule the operator. No free resource available capacity is="
                  << sinkNode->getRemainingCpuCapacity());
        throw std::runtime_error("Can not schedule the operator. No free resource available.");
    }
}

}// namespace NES

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
#ifdef TFDEF

#ifndef NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MLHEURISTICSTRATEGY_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MLHEURISTICSTRATEGY_HPP_

#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <iostream>

namespace NES {

class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace NES

namespace NES::Optimizer {

/**
 * @brief
 *          This class implements Bottom Up placement strategy. In this strategy, the source and sink operators are
 *          placed at respective nes nodes but rest of the operators are placed starting near to the source and then
 *          if the resources are not available they are placed on a node neighbouring to the node or one level up.
 */
class MlHeuristicStrategy : public BasePlacementStrategy {
  public:
    ~MlHeuristicStrategy() override = default;

    static const bool DEFAULT_ENABLE_OPERATOR_REDUNDANCY_ELIMINATION = false;
    static const bool DEFAULT_ENABLE_CPU_SAVER_MODE = true;
    static const int DEFAULT_MIN_RESOURCE_LIMIT = 5;
    static const bool DEFAULT_LOW_THROUGHPUT_SOURCE = false;
    static const bool DEFAULT_ML_HARDWARE = false;

    /**
     * @brief Implementation of the virtual function of BasePlacementStrategy
     * @param queryId
     * @param faultToleranceType
     * @param lineageType
     * @param pinnedUpStreamOperators
     * @param pinnedDownStreamOperators
     * @return
     */
    bool updateGlobalExecutionPlan(QueryId queryId,
                                   FaultToleranceType::Value faultToleranceType,
                                   LineageType::Value lineageType,
                                   const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                   const std::vector<OperatorNodePtr>& pinnedDownStreamOperators) override;

    /**
     * @brief creates an Object of this class through a static create function
     * @param globalExecutionPlan
     * @param topology
     * @param typeInferencePhase
     * @return
     */
    static std::unique_ptr<BasePlacementStrategy>
    create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, TypeInferencePhasePtr typeInferencePhase);

  private:
    explicit MlHeuristicStrategy(GlobalExecutionPlanPtr globalExecutionPlan,
                                 TopologyPtr topology,
                                 TypeInferencePhasePtr typeInferencePhase);

    void performOperatorPlacement(QueryId queryId,
                                  const std::vector<OperatorNodePtr>& pinnedUpStreamOperators,
                                  const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    void identifyPinningLocation(QueryId queryId,
                                 const OperatorNodePtr& operatorNode,
                                 TopologyNodePtr candidateTopologyNode,
                                 const std::vector<OperatorNodePtr>& pinnedDownStreamOperators);

    /**
     * @brief checks if the ratio of #sink_fields/#source_fields > 1/product of all selectivities
     * @param operatorNode
     * @return
     */
    bool pushUpBasedOnFilterSelectivity(const OperatorNodePtr& operatorNode);

    /**
     * @brief removes redundant operators
     * @param queryId
     * @param faultToleranceType
     * @param lineageType
     */
    void performOperatorRedundancyElimination(QueryId queryId,
                                              FaultToleranceType::Value faultToleranceType,
                                              LineageType::Value lineageType);
};
}// namespace NES::Optimizer

#endif//MLHEURISTIC_HPP

#endif// NES_CORE_INCLUDE_OPTIMIZER_QUERYPLACEMENT_MLHEURISTICSTRATEGY_HPP_

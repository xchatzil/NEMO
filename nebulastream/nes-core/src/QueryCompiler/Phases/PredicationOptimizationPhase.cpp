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
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferScan.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperatorPredicated.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableMapOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableProjectionOperator.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Phases/PredicationOptimizationPhase.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>

namespace NES::QueryCompilation {

PredicationOptimizationPhasePtr PredicationOptimizationPhase::PredicationOptimizationPhase::create(
    QueryCompilerOptions::FilterProcessingStrategy filterProcessingStrategy) {
    return std::make_shared<PredicationOptimizationPhase>(filterProcessingStrategy);
}

PredicationOptimizationPhase::PredicationOptimizationPhase(
    QueryCompilerOptions::FilterProcessingStrategy filterProcessingStrategy)
    : filterProcessingStrategy(filterProcessingStrategy) {}

PipelineQueryPlanPtr PredicationOptimizationPhase::apply(PipelineQueryPlanPtr pipelinedQueryPlan) {
    if (filterProcessingStrategy == QueryCompilerOptions::BRANCHED) {
        NES_DEBUG("PredicationOptimizationPhase: No optimization requested or applied.");
        return pipelinedQueryPlan;
    }

    for (const auto& pipeline : pipelinedQueryPlan->getPipelines()) {
        if (pipeline->isOperatorPipeline()) {
            apply(pipeline);
        }
    }
    return pipelinedQueryPlan;
}

OperatorPipelinePtr PredicationOptimizationPhase::apply(OperatorPipelinePtr operatorPipeline) {
    if (filterProcessingStrategy == QueryCompilerOptions::BRANCHED) {
        NES_DEBUG("PredicationOptimizationPhase: No optimization requested or applied.");
        return operatorPipeline;
    }

    NES_DEBUG("PredicationOptimizationPhase: Scanning pipeline for optimization potential.");
    auto queryPlan = operatorPipeline->getQueryPlan();
    auto nodes = QueryPlanIterator(queryPlan).snapshot();

    // TODO enable predication based on a cost model and data characteristics

    // abort if invalid operator is found:
    for (const auto& node : nodes) {
        if (!node->instanceOf<GeneratableOperators::GeneratableBufferEmit>()
            && !node->instanceOf<GeneratableOperators::GeneratableBufferScan>()
            && !node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()
            && !node->instanceOf<GeneratableOperators::GeneratableMapOperator>()
            && !node->instanceOf<GeneratableOperators::GeneratableProjectionOperator>()) {
            NES_DEBUG("PredicationOptimizationPhase: No predication applied. There is an unsupported operator in the pipeline: "
                      + node->toString());
            return operatorPipeline;
        }
    }
    // replace all filter operators with predicated filter operators:
    for (const auto& node : nodes) {
        if (node->instanceOf<GeneratableOperators::GeneratableFilterOperator>()) {
            auto filterOperator = node->as<GeneratableOperators::GeneratableFilterOperator>();
            auto predicatedFilterOperator =
                GeneratableOperators::GeneratableFilterOperatorPredicated::create(filterOperator->getInputSchema(),
                                                                                  filterOperator->getPredicate());
            queryPlan->replaceOperator(filterOperator, predicatedFilterOperator);
            NES_DEBUG("PredicationOptimizationPhase: Replaced filter operator with equivalent predicated filter operator.");
        }
    }

    return operatorPipeline;
}

}// namespace NES::QueryCompilation
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
#include <Plans/Query/QueryPlan.hpp>
#include <QueryCompiler/Exceptions/QueryCompilationException.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/CEP/GeneratableCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferEmit.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableBufferScan.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableFilterOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableInferModelOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableMapOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableProjectionOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/GeneratableWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableBatchJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableBatchJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Joining/GeneratableJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableAvgAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableCountAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMaxAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMedianAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableMinAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableSumAggregation.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GeneratableSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/GlobalTimeWindow/GeneratableGlobalWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/KeyedTimeWindow/GeneratableKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/KeyedTimeWindow/GeneratableKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/KeyedTimeWindow/GeneratableKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/KeyedTimeWindow/GeneratableKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/KeyedTimeWindow/GeneratableKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/CEP/PhysicalCEPIterationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalBatchJoinProbeOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinBuildOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Joining/PhysicalJoinSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalExternalOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalFilterOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalProjectOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalPythonUdfOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalSourceOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalWatermarkAssignmentOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/GlobalTimeWindow/PhysicalGlobalWindowSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedGlobalSliceStoreAppendOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedSlidingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedThreadLocalPreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/KeyedTimeWindow/PhysicalKeyedTumblingWindowSink.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceMergingOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSlicePreAggregationOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalSliceSinkOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/Windowing/PhysicalWindowSinkOperator.hpp>
#include <QueryCompiler/Phases/Translations/DefaultGeneratableOperatorProvider.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/Experimental/GlobalTimeWindow/GlobalWindowGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedGlobalSliceStoreAppendOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceMergingOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlidingWindowSinkOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowAggregations/CountAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MaxAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/MinAggregationDescriptor.hpp>
#include <Windowing/WindowAggregations/SumAggregationDescriptor.hpp>
#include <Windowing/WindowHandler/WindowOperatorHandler.hpp>
#include <utility>
#ifdef PYTHON_UDF_ENABLED
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalPythonUdfOperator.hpp>
#endif

class PhysicalCEPIterationOperator;
namespace NES::QueryCompilation {

GeneratableOperatorProviderPtr DefaultGeneratableOperatorProvider::create() {
    return std::make_shared<DefaultGeneratableOperatorProvider>();
}

void DefaultGeneratableOperatorProvider::lower(QueryPlanPtr queryPlan, PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    NES_DEBUG("Lower " << operatorNode->toString() << " to generatable operator");
    if (operatorNode->instanceOf<PhysicalOperators::PhysicalSourceOperator>()) {
        lowerSource(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSinkOperator>()) {
        lowerSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalScanOperator>()) {
        lowerScan(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalEmitOperator>()) {
        lowerEmit(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalProjectOperator>()) {
        lowerProjection(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalFilterOperator>()) {
        lowerFilter(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalInferModelOperator>()) {
        lowerInferModel(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalMapOperator>()) {
        lowerMap(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalIterationCEPOperator>()) {
        lowerCEPIteration(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWatermarkAssignmentOperator>()) {
        lowerWatermarkAssignment(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSlicePreAggregationOperator>()) {
        lowerSlicePreAggregation(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalWindowSinkOperator>()) {
        lowerWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSliceSinkOperator>()) {
        lowerSliceSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalSliceMergingOperator>()) {
        lowerSliceMerging(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalJoinBuildOperator>()) {
        lowerJoinBuild(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalJoinSinkOperator>()) {
        lowerJoinSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>()) {
        return lowerKeyedThreadLocalSlicePreAggregation(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedSliceMergingOperator>()) {
        return lowerKeyedSliceMergingOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedTumblingWindowSink>()) {
        return lowerKeyedTumblingWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedSlidingWindowSink>()) {
        return lowerKeyedSlidingWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator>()) {
        return lowerKeyedGlobalSliceStoreAppendOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>()) {
        return lowerGlobalThreadLocalSlicePreAggregation(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalSliceMergingOperator>()) {
        return lowerGlobalSliceMergingOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalTumblingWindowSink>()) {
        return lowerGlobalTumblingWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalSlidingWindowSink>()) {
        return lowerGlobalSlidingWindowSink(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalGlobalWindowSliceStoreAppendOperator>()) {
        return lowerGlobalWindowSliceStoreAppendOperator(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalBatchJoinBuildOperator>()) {
        lowerBatchJoinBuild(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::Experimental::PhysicalBatchJoinProbeOperator>()) {
        lowerBatchJoinProbe(queryPlan, operatorNode);
    } else if (operatorNode->instanceOf<PhysicalOperators::PhysicalExternalOperator>()) {
        return;
    }
#ifdef PYTHON_UDF_ENABLED
    else if (operatorNode->instanceOf<PhysicalOperators::Experimental::PhysicalPythonUdfOperator>()) {
        return;
    }
#endif
    else {
        throw QueryCompilationException("No lowering defined for physical operator: " + operatorNode->toString());
    }
}

void DefaultGeneratableOperatorProvider::lowerSink(const QueryPlanPtr&,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    // a sink operator should be in a pipeline on its own.
    NES_ASSERT(operatorNode->getChildren().size(), "A sink node should have no children");
    NES_ASSERT(operatorNode->getParents().size(), "A sink node should have no parents");
}

void DefaultGeneratableOperatorProvider::lowerSource(const QueryPlanPtr&,
                                                     const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    // a source operator should be in a pipeline on its own.
    NES_ASSERT(operatorNode->getChildren().size(), "A source operator should have no children");
    NES_ASSERT(operatorNode->getParents().size(), "A source operator should have no parents");
}

void DefaultGeneratableOperatorProvider::lowerScan(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto bufferScan = GeneratableOperators::GeneratableBufferScan::create(operatorNode->getOutputSchema());
    queryPlan->replaceOperator(operatorNode, bufferScan);
}

void DefaultGeneratableOperatorProvider::lowerEmit(const QueryPlanPtr& queryPlan,
                                                   const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto bufferEmit = GeneratableOperators::GeneratableBufferEmit::create(operatorNode->getOutputSchema());
    queryPlan->replaceOperator(operatorNode, bufferEmit);
}

void DefaultGeneratableOperatorProvider::lowerProjection(const QueryPlanPtr& queryPlan,
                                                         const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalProjectionOperator = operatorNode->as<PhysicalOperators::PhysicalProjectOperator>();
    auto generatableProjectOperator =
        GeneratableOperators::GeneratableProjectionOperator::create(physicalProjectionOperator->getInputSchema(),
                                                                    physicalProjectionOperator->getOutputSchema(),
                                                                    physicalProjectionOperator->getExpressions());
    queryPlan->replaceOperator(physicalProjectionOperator, generatableProjectOperator);
}

void DefaultGeneratableOperatorProvider::lowerFilter(const QueryPlanPtr& queryPlan,
                                                     const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalFilterOperator = operatorNode->as<PhysicalOperators::PhysicalFilterOperator>();
    auto generatableFilterOperator =
        GeneratableOperators::GeneratableFilterOperator::create(physicalFilterOperator->getInputSchema(),
                                                                physicalFilterOperator->getPredicate());
    queryPlan->replaceOperator(physicalFilterOperator, generatableFilterOperator);
}

void DefaultGeneratableOperatorProvider::lowerInferModel(QueryPlanPtr queryPlan,
                                                         PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    auto physicalInferModelOperator = operatorNode->as<PhysicalOperators::PhysicalInferModelOperator>();
    auto generatableInferModelOperator =
        GeneratableOperators::GeneratableInferModelOperator::create(physicalInferModelOperator->getInputSchema(),
                                                                    physicalInferModelOperator->getOutputSchema(),
                                                                    physicalInferModelOperator->getModel(),
                                                                    physicalInferModelOperator->getInputFields(),
                                                                    physicalInferModelOperator->getOutputFields(),
                                                                    physicalInferModelOperator->getInferModelHandler());
    queryPlan->replaceOperator(physicalInferModelOperator, generatableInferModelOperator);
}

void DefaultGeneratableOperatorProvider::lowerMap(const QueryPlanPtr& queryPlan,
                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalMapOperator = operatorNode->as<PhysicalOperators::PhysicalMapOperator>();
    auto generatableMapOperator = GeneratableOperators::GeneratableMapOperator::create(physicalMapOperator->getInputSchema(),
                                                                                       physicalMapOperator->getOutputSchema(),
                                                                                       physicalMapOperator->getMapExpression());
    queryPlan->replaceOperator(physicalMapOperator, generatableMapOperator);
}

void DefaultGeneratableOperatorProvider::lowerWatermarkAssignment(const QueryPlanPtr& queryPlan,
                                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalWatermarkAssignmentOperator = operatorNode->as<PhysicalOperators::PhysicalWatermarkAssignmentOperator>();
    auto generatableWatermarkAssignmentOperator = GeneratableOperators::GeneratableWatermarkAssignmentOperator::create(
        physicalWatermarkAssignmentOperator->getInputSchema(),
        physicalWatermarkAssignmentOperator->getOutputSchema(),
        physicalWatermarkAssignmentOperator->getWatermarkStrategyDescriptor());
    queryPlan->replaceOperator(physicalWatermarkAssignmentOperator, generatableWatermarkAssignmentOperator);
}

GeneratableOperators::GeneratableWindowAggregationPtr DefaultGeneratableOperatorProvider::lowerWindowAggregation(
    const Windowing::WindowAggregationDescriptorPtr& windowAggregationDescriptor) {
    switch (windowAggregationDescriptor->getType()) {
        case Windowing::WindowAggregationDescriptor::Count: {
            return GeneratableOperators::GeneratableCountAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Max: {
            return GeneratableOperators::GeneratableMaxAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Min: {
            return GeneratableOperators::GeneratableMinAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Sum: {
            return GeneratableOperators::GeneratableSumAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Avg: {
            return GeneratableOperators::GeneratableAvgAggregation::create(windowAggregationDescriptor);
        };
        case Windowing::WindowAggregationDescriptor::Median: {
            return GeneratableOperators::GeneratableMedianAggregation::create(windowAggregationDescriptor);
        };
        default:
            throw QueryCompilationException(
                "TranslateToGeneratableOperatorPhase: No transformation implemented for this window aggregation type: "
                + windowAggregationDescriptor->toString());
    }
}

void DefaultGeneratableOperatorProvider::lowerSlicePreAggregation(const QueryPlanPtr& queryPlan,
                                                                  const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto slicePreAggregationOperator = operatorNode->as<PhysicalOperators::PhysicalSlicePreAggregationOperator>();

    auto windowAggregations = slicePreAggregationOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregations) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator =
        GeneratableOperators::GeneratableSlicePreAggregationOperator::create(slicePreAggregationOperator->getInputSchema(),
                                                                             slicePreAggregationOperator->getOutputSchema(),
                                                                             slicePreAggregationOperator->getOperatorHandler(),
                                                                             generatableAggregations);
    queryPlan->replaceOperator(slicePreAggregationOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerSliceMerging(const QueryPlanPtr& queryPlan,
                                                           const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sliceMergingOperator = operatorNode->as<PhysicalOperators::PhysicalSliceMergingOperator>();

    auto windowAggregationDescriptor = sliceMergingOperator->getOperatorHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }

    auto generatableOperator =
        GeneratableOperators::GeneratableSliceMergingOperator::create(sliceMergingOperator->getInputSchema(),
                                                                      sliceMergingOperator->getOutputSchema(),
                                                                      sliceMergingOperator->getOperatorHandler(),
                                                                      generatableAggregations);
    queryPlan->replaceOperator(sliceMergingOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerWindowSink(QueryPlanPtr queryPlan,
                                                         PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    // a window sink is lowered to a standard scan operator
    lowerScan(std::move(queryPlan), std::move(operatorNode));
}

void DefaultGeneratableOperatorProvider::lowerSliceSink(QueryPlanPtr queryPlan,
                                                        PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    // a slice sink is lowered to a standard scan operator
    lowerScan(std::move(queryPlan), std::move(operatorNode));
}

void DefaultGeneratableOperatorProvider::lowerJoinBuild(const QueryPlanPtr& queryPlan,
                                                        const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalJoinBuild = operatorNode->as<PhysicalOperators::PhysicalJoinBuildOperator>();
    auto generatableJoinOperator =
        GeneratableOperators::GeneratableJoinBuildOperator::create(physicalJoinBuild->getInputSchema(),
                                                                   physicalJoinBuild->getOutputSchema(),
                                                                   physicalJoinBuild->getJoinHandler(),
                                                                   physicalJoinBuild->getBuildSide());
    queryPlan->replaceOperator(operatorNode, generatableJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerJoinSink(const QueryPlanPtr& queryPlan,
                                                       const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalJoinSink = operatorNode->as<PhysicalOperators::PhysicalJoinSinkOperator>();
    auto generatableJoinOperator = GeneratableOperators::GeneratableJoinSinkOperator::create(physicalJoinSink->getOutputSchema(),
                                                                                             physicalJoinSink->getOutputSchema(),
                                                                                             physicalJoinSink->getJoinHandler());
    queryPlan->replaceOperator(operatorNode, generatableJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerBatchJoinBuild(const QueryPlanPtr& queryPlan,
                                                             const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalBatchJoinBuild = operatorNode->as<PhysicalOperators::PhysicalBatchJoinBuildOperator>();
    auto generatableBatchJoinOperator =
        GeneratableOperators::GeneratableBatchJoinBuildOperator::create(physicalBatchJoinBuild->getInputSchema(),
                                                                        physicalBatchJoinBuild->getOutputSchema(),
                                                                        physicalBatchJoinBuild->getBatchJoinHandler());
    queryPlan->replaceOperator(operatorNode, generatableBatchJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerBatchJoinProbe(const QueryPlanPtr& queryPlan,
                                                             const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto physicalBatchJoinSink = operatorNode->as<PhysicalOperators::Experimental::PhysicalBatchJoinProbeOperator>();
    auto generatableBatchJoinOperator =
        GeneratableOperators::GeneratableBatchJoinProbeOperator::create(physicalBatchJoinSink->getOutputSchema(),
                                                                        physicalBatchJoinSink->getOutputSchema(),
                                                                        physicalBatchJoinSink->getBatchJoinHandler());
    queryPlan->replaceOperator(operatorNode, generatableBatchJoinOperator);
}

void DefaultGeneratableOperatorProvider::lowerCEPIteration(QueryPlanPtr queryPlan,
                                                           PhysicalOperators::PhysicalOperatorPtr operatorNode) {
    auto physicalCEPIterationOperator = operatorNode->as<PhysicalOperators::PhysicalIterationCEPOperator>();
    auto generatableCEPIterationOperator =
        GeneratableOperators::GeneratableCEPIterationOperator::create(physicalCEPIterationOperator->getInputSchema(),
                                                                      physicalCEPIterationOperator->getOutputSchema(),
                                                                      physicalCEPIterationOperator->getMinIterations(),
                                                                      physicalCEPIterationOperator->getMaxIterations());
    queryPlan->replaceOperator(physicalCEPIterationOperator, generatableCEPIterationOperator);
}

void DefaultGeneratableOperatorProvider::lowerKeyedThreadLocalSlicePreAggregation(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {

    auto slicePreAggregationOperator = operatorNode->as<PhysicalOperators::PhysicalKeyedThreadLocalPreAggregationOperator>();

    auto windowAggregationDescriptors =
        slicePreAggregationOperator->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptors) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableKeyedThreadLocalPreAggregationOperator::create(
        slicePreAggregationOperator->getInputSchema(),
        slicePreAggregationOperator->getOutputSchema(),
        slicePreAggregationOperator->getWindowHandler(),
        generatableAggregations);
    queryPlan->replaceOperator(slicePreAggregationOperator, generatableOperator);
}
void DefaultGeneratableOperatorProvider::lowerKeyedSliceMergingOperator(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sliceMergingOperator = operatorNode->as<PhysicalOperators::PhysicalKeyedSliceMergingOperator>();

    auto windowAggregationDescriptor = sliceMergingOperator->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator =
        GeneratableOperators::GeneratableKeyedSliceMergingOperator::create(sliceMergingOperator->getInputSchema(),
                                                                           sliceMergingOperator->getOutputSchema(),
                                                                           sliceMergingOperator->getWindowHandler(),
                                                                           generatableAggregations);
    queryPlan->replaceOperator(sliceMergingOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerKeyedTumblingWindowSink(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalKeyedTumblingWindowSink>();

    auto windowAggregationDescriptor = sink->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableKeyedTumblingWindowSink::create(sink->getInputSchema(),
                                                                                                sink->getOutputSchema(),
                                                                                                sink->getWindowDefinition(),
                                                                                                generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}
void DefaultGeneratableOperatorProvider::lowerKeyedSlidingWindowSink(const QueryPlanPtr& queryPlan,
                                                                     const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalKeyedSlidingWindowSink>();

    auto windowAggregationDescriptor = sink->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableKeyedSlidingWindowSink::create(sink->getInputSchema(),
                                                                                               sink->getOutputSchema(),
                                                                                               sink->getWindowHandler(),
                                                                                               generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerKeyedGlobalSliceStoreAppendOperator(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalKeyedGlobalSliceStoreAppendOperator>();

    auto windowAggregationDescriptor = sink->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator =
        GeneratableOperators::GeneratableKeyedGlobalSliceStoreAppendOperator::create(sink->getInputSchema(),
                                                                                     sink->getOutputSchema(),
                                                                                     sink->getWindowHandler(),
                                                                                     generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerGlobalThreadLocalSlicePreAggregation(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {

    auto slicePreAggregationOperator = operatorNode->as<PhysicalOperators::PhysicalGlobalThreadLocalPreAggregationOperator>();

    auto windowAggregationDescriptors =
        slicePreAggregationOperator->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptors) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableGlobalThreadLocalPreAggregationOperator::create(
        slicePreAggregationOperator->getInputSchema(),
        slicePreAggregationOperator->getOutputSchema(),
        slicePreAggregationOperator->getWindowHandler(),
        generatableAggregations);
    queryPlan->replaceOperator(slicePreAggregationOperator, generatableOperator);
}
void DefaultGeneratableOperatorProvider::lowerGlobalSliceMergingOperator(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sliceMergingOperator = operatorNode->as<PhysicalOperators::PhysicalGlobalSliceMergingOperator>();

    auto windowAggregationDescriptor = sliceMergingOperator->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator =
        GeneratableOperators::GeneratableGlobalSliceMergingOperator::create(sliceMergingOperator->getInputSchema(),
                                                                            sliceMergingOperator->getOutputSchema(),
                                                                            sliceMergingOperator->getWindowHandler(),
                                                                            generatableAggregations);
    queryPlan->replaceOperator(sliceMergingOperator, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerGlobalTumblingWindowSink(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalGlobalTumblingWindowSink>();

    auto windowAggregationDescriptor = sink->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableGlobalTumblingWindowSink::create(sink->getInputSchema(),
                                                                                                 sink->getOutputSchema(),
                                                                                                 sink->getWindowDefinition(),
                                                                                                 generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}
void DefaultGeneratableOperatorProvider::lowerGlobalSlidingWindowSink(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalGlobalSlidingWindowSink>();

    auto windowAggregationDescriptor = sink->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator = GeneratableOperators::GeneratableGlobalSlidingWindowSink::create(sink->getInputSchema(),
                                                                                                sink->getOutputSchema(),
                                                                                                sink->getWindowHandler(),
                                                                                                generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}

void DefaultGeneratableOperatorProvider::lowerGlobalWindowSliceStoreAppendOperator(
    const QueryPlanPtr& queryPlan,
    const PhysicalOperators::PhysicalOperatorPtr& operatorNode) {
    auto sink = operatorNode->as<PhysicalOperators::PhysicalGlobalWindowSliceStoreAppendOperator>();

    auto windowAggregationDescriptor = sink->getWindowHandler()->getWindowDefinition()->getWindowAggregation();
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> generatableAggregations;
    for (auto agg : windowAggregationDescriptor) {
        generatableAggregations.emplace_back(lowerWindowAggregation(agg));
    }
    auto generatableOperator =
        GeneratableOperators::GeneratableGlobalWindowSliceStoreAppendOperator::create(sink->getInputSchema(),
                                                                                      sink->getOutputSchema(),
                                                                                      sink->getWindowHandler(),
                                                                                      generatableAggregations);
    queryPlan->replaceOperator(sink, generatableOperator);
}

}// namespace NES::QueryCompilation
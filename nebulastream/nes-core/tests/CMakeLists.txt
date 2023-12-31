
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Google Testing Framework ----------------------------------------------------
include(ExternalProject)
if (CMAKE_CROSSCOMPILING)
    set(CMAKE_ARGS_CROSS -DCMAKE_BUILD_TYPE=Release
            -DCMAKE_TOOLCHAIN_FILE=/opt/toolchain/toolchain-aarch64-llvm.cmake
            -DBoost_NO_SYSTEM_PATHS=TRUE
            -DBoost_INCLUDE_DIR=/opt/sysroots/aarch64-linux-gnu/usr/include/
            -DBoost_LIBRARY_DIR=/opt/sysroots/aarch64-linux-gnu/usr/lib/
            )
else ()
    set(CMAKE_ARGS_CROSS "")
endif ()

# Copy Test Data --------------------------------------------------------------
file(COPY test_data DESTINATION .)
add_compile_definitions(TEST_DATA_DIRECTORY="${CMAKE_CURRENT_BINARY_DIR}/test_data/")
message(STATUS "Test Data Directory: ${TEST_DATA_DIRECTORY}/test_data")

#-------------------------------##############----------------------------------
#------------------------------- # Nes Utils for Test Suite --------------------------------

add_library(nes-tests INTERFACE)
target_include_directories(nes-tests INTERFACE ${GTEST_INCLUDE_DIRS} "../include")

target_include_directories(nes-tests INTERFACE "${CMAKE_BINARY_DIR}")
message(STATUS "Test CMAKE_CURRENT_SOURCE_DIR Directory: ${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(nes-tests INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include/")
target_link_libraries(nes-tests INTERFACE ${GTEST_LIBRARIES} nes nes-tests-util)

if (CODE_COVERAGE)
    target_code_coverage(nes AUTO)
endif ()

#-------------------------------##############----------------------------------
#------------------------------- # Unit Test Suite --------------------------------
add_nes_unit_test(udf-test "UnitTests/Catalogs/UdfCatalogTest.cpp" "UnitTests/Catalogs/JavaUdfDescriptorTest.cpp" "UnitTests/Catalogs/PythonUdfDescriptorTest.cpp")


add_nes_unit_test(hashmap-test "UnitTests/Util/HashMapTest.cpp")


### Node Engine Tests ###
add_nes_integration_test(node-engine-test "UnitTests/Runtime/NodeEngineTest.cpp")


add_nes_unit_test(watermark-manager-test "UnitTests/Runtime/WatermarkManagerTest.cpp")


add_nes_unit_test(lock-free-multi-origin-watermark-processor-test "UnitTests/Windowing/Experimental/LockFreeMultiOriginWatermarkProcessorTest.cpp")


add_nes_unit_test(keyed-thread-local-slice-store-test "UnitTests/Windowing/Experimental/TimeBasedWindow/KeyedThreadLocalSliceStoreTest.cpp")


add_nes_unit_test(window-assigner-test "UnitTests/Windowing/Experimental/WindowAssignerTest.cpp")


### Code Generation Tests ###
add_nes_unit_test(code-generation-tests "UnitTests/CodeGenerator/CodeGenerationTest.cpp")


### Translate Physical Operator Tests ###
add_nes_unit_test(translate-physical-operator-tests "UnitTests/QueryCompiler/LowerLogicalToPhysicalOperatorsTest.cpp")


### AddScanAndEmitPhase Test ###
add_nes_unit_test(add-scan-and-emit-phase-tests "UnitTests/QueryCompiler/AddScanAndEmitPhaseTest.cpp")


### ArrayType Test ###
add_nes_unit_test(array-type-tests "UnitTests/QueryCompiler/ArrayTypeTest.cpp")


### DataTypeFactory Test ###
add_nes_unit_test(data-type-factory-tests "UnitTests/QueryCompiler/DataTypeFactoryTests.cpp")


### Pipelining Tests ###
add_nes_unit_test(pipelining-phase-tests "UnitTests/QueryCompiler/PipeliningPhaseTest.cpp")


### Operator Code Generation Tests ###
add_nes_unit_test(operator-code-generation-tests "UnitTests/CodeGenerator/OperatorCodeGenerationTest.cpp")


### Query Compiler Tests ###
add_nes_unit_test(query-compiler-tests "UnitTests/QueryCompiler/QueryCompilerTest.cpp")


### NES Topology Tests ###
add_nes_unit_test(topology-tests "UnitTests/Topology/TopologyTest.cpp")

#-------------------------------##############----------------------------------
#------------------------------- # Integration Test Suite --------------------------------
### NES Connectivity Controller Tests ###
add_nes_integration_test(connectivity-controller-integration-tests "Integration/REST/Controller/ConnectivityControllerTest.cpp")

### NES Query Catalog Controller Test ###
add_nes_integration_test(query-catalog-controller-integration-tests "Integration/REST/Controller/QueryCatalogControllerTest.cpp")

### NES Monitoring Controller Test ###
add_nes_integration_test(monitoring-controller-tests "Integration/REST/Controller/MonitoringControllerTest.cpp")

### NES Topology Controller Test ###
add_nes_integration_test(topology-controller-integration-tests "Integration/REST/Controller/TopologyControllerTest.cpp")

### NES Udf Catalog Controller Test ###
add_nes_integration_test(udf-catalog-controller-integration-tests "Integration/REST/Controller/UdfCatalogControllerTest.cpp")

### NES Query Controller Test ###
add_nes_integration_test(query-controller-integration-tests "Integration/REST/Controller/QueryControllerTest.cpp")

### NES Source Catalog Controller Test ###
add_nes_integration_test(source-catalog-controller-integration-tests "Integration/REST/Controller/SourceCatalogControllerTest.cpp")

### NES Maintenance Controller Tests ###
add_nes_integration_test(maintenance-controller-integration-test "Integration/REST/Controller/MaintenanceControllerTest.cpp")

### NES Location Controller Tests ###
add_nes_integration_test(location-controller-integration-tests "Integration/REST/Controller/LocationControllerTest.cpp")

### NES TopologyProperty Tests ###
add_nes_unit_test(topology-property-tests "UnitTests/Topology/TopologyPropertyTest.cpp")

### NES TopologyIterator Test ###
add_nes_unit_test(topology-iterator-tests "UnitTests/Topology/TopologyIterationTest.cpp")

### Lineage Manager Test ###
add_nes_unit_test(lineage-manager-tests "UnitTests/Runtime/LineageManagerTest.cpp")


### Buffer Manager Test ###
add_nes_unit_test(buffer-manager-tests "UnitTests/Runtime/BufferManagerTest.cpp")


### Buffer Storage Test ###
add_nes_unit_test(buffer-storage-tests "UnitTests/Runtime/BufferStorageTest.cpp")


### AbstractWindowHandler Manager Test ###
add_nes_unit_test(window-manager-tests "UnitTests/Windowing/WindowManagerTest.cpp")


### State Manager Test ###
add_nes_unit_test(state-manager-tests "UnitTests/State/StateTest.cpp")


### ZMQ Data Source Sink Tests ###
add_nes_unit_test(zmq-tests "UnitTests/Source/ZMQTest.cpp")

### Source Tests ###
add_nes_integration_test(source-tests "UnitTests/Source/SourceTest.cpp")

### Sink Tests ###
add_nes_unit_test(sink-tests "UnitTests/Sink/SinkTest.cpp" UnitTests/Sink/SinkTest.cpp)

### Kafka Tests ###
if (NES_USE_KAFKA)
    add_nes_unit_test(kafka-source-tests "UnitTests/Source/KafkaSourceTest.cpp")
    add_nes_unit_test(kafka-sink-tests "UnitTests/Sink/KafkaSinkTest.cpp")
endif (NES_USE_KAFKA)

### OPC Tests ###
if (NES_USE_OPC)
    add_nes_unit_test(opc-source-tests "UnitTests/Source/OPCSourceTest.cpp" UnitTests/Source/OPCSourceTest.cpp)
    add_nes_unit_test(opc-sink-tests "UnitTests/Sink/OPCSinkTest.cpp" UnitTests/Sink/OPCSinkTest.cpp)
endif (NES_USE_OPC)

### MQTT Tests ###
if (NES_USE_MQTT)
    add_nes_unit_test(mqtt-source-tests "UnitTests/Source/MQTTSourceTest.cpp" UnitTests/Source/MQTTSourceTest.cpp)
    add_nes_unit_test(mqtt-sink-tests "UnitTests/Sink/MQTTSinkTest.cpp")
endif (NES_USE_MQTT)


### Row Layout Tests ###
add_nes_unit_test(row-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/RowMemoryLayoutTest.cpp")


### Row Layout Tests ###
add_nes_unit_test(columnar-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/ColumnarMemoryLayoutTest.cpp")


### Dynamic Memory Layout Tests ###
add_nes_unit_test(dynamic-memory-layout-tests "UnitTests/Runtime/MemoryLayouts/DynamicMemoryLayoutTest.cpp")


### Source Catalog Tests ###
add_nes_unit_test(source-catalog-test "UnitTests/Catalogs/SourceCatalogTest.cpp")


### QueryCatalog Tests ###
add_nes_unit_test(query-catalog-service-tests "UnitTests/Services/QueryCatalogServiceTest.cpp")


### LocationService Tests ###
add_nes_unit_test(location-service-tests "UnitTests/Services/LocationServiceTest.cpp")


### LogicalExpressionTest ###
add_nes_unit_test(logical-expression-test "UnitTests/Query/LogicalExpressionTest.cpp")


### QueryExecutionTest ###
add_nes_unit_test(query-execution-test "UnitTests/Query/QueryExecutionTest.cpp")


### GPUPipelineQueryExecutionTest ###
if (NES_USE_GPU)
    add_nes_unit_test(gpu-pipeline-query-execution-test "UnitTests/Query/GPUPipelineQueryExecutionTest.cpp")
endif (NES_USE_GPU)

### ProjectionTest ###
add_nes_unit_test(projection-test "UnitTests/Query/ProjectionTest.cpp")


### TupleBufferTest ###
add_nes_unit_test(tuple-buffer-test "UnitTests/Runtime/TupleBufferTest.cpp")


if (CMAKE_USE_PAPI_PROFILER)
    add_nes_unit_test(papi-profiler-test "UnitTests/Runtime/PAPIProfilerTest.cpp")
endif ()

# LogicalOperatorPlanNodeTest
add_nes_unit_test(logical-operator-node-tests
        "UnitTests/Operators/LogicalOperatorNodeTest.cpp"
        "UnitTests/Operators/LogicalOperators/MapJavaUdfLogicalOperatorNodeTest.cpp")


# ExpressionNodeTest
add_nes_unit_test(expression-node-test "UnitTests/Operators/ExpressionNodeTest.cpp")


# OperatorPropertiesTest
add_nes_unit_test(operator-properties-tests "UnitTests/Operators/OperatorPropertiesTest.cpp")


# QueryTest
add_nes_unit_test(query-tests "UnitTests/API/QueryAPITest.cpp")


# SpatialQueryTest
add_nes_unit_test(spatial-query-tests "UnitTests/API/SpatialQueryTest.cpp")

# SpatialExpressionNodeTest
add_nes_unit_test(spatial-expression-node-test "UnitTests/Operators/SpatialExpressionNodeTest.cpp")

# GeographyExpressionNodeTest
add_nes_unit_test(geography-expression-node-test "UnitTests/Operators/GeographyExpressionNodeTest.cpp")


# QueryPlanIteratorTest
add_nes_unit_test(queryplan-util-iterator-test "UnitTests/Plans/Util/QueryPlanIteratorTest.cpp")


# ConvertLogicalToPhysicalSinkTest
add_nes_unit_test(convert-logical-to-physical-sink-tests "UnitTests/Phases/ConvertLogicalToPhysicalSinkTest.cpp")


# ConvertLogicalToPhysicalSourceTest
add_nes_unit_test(convert-logical-to-physical-source-tests "UnitTests/Phases/ConvertLogicalToPhysicalSourceTest.cpp")


# Type inference test
add_nes_unit_test(type-inference-tests "UnitTests/Optimizer/Phases/TypeInferencePhaseTest.cpp")


# FilterPushDownTest
add_nes_unit_test(filter-push-down-rule-test "UnitTests/Optimizer/QueryRewrite/FilterPushDownRuleTest.cpp")


# DistributeWindowRuleTest
add_nes_unit_test(distribute-window-rule-test "UnitTests/Optimizer/QueryRewrite/DistributeWindowRuleTest.cpp")


# DistributeWindowRuleTest
add_nes_unit_test(originid-inference-phase-test "UnitTests/Optimizer/Phases/OriginIdInferencePhaseTest.cpp")


# SerializationUtilTest
add_nes_unit_test(serialize-operator-test "UnitTests/Serialization/SerializationUtilTest.cpp")


### SourceCatalogService Tests ###
add_nes_unit_test(source-catalog-service-tests "UnitTests/Services/SourceCatalogServiceTest.cpp")


### TopologyManagerService Tests ###
add_nes_unit_test(topology-manager-service-tests "UnitTests/Services/TopologyManagerServiceTest.cpp")


### MaintenanceService Tests ###
add_nes_unit_test(maintenance-service-tests "UnitTests/Services/MaintenanceServiceTest.cpp")


### Query Plan Tests ###
add_nes_unit_test(query-plan-tests "UnitTests/Plans/Query/QueryPlanTest.cpp")

### Query Plan Builder Tests ###
add_nes_unit_test(query-plan-builder-tests "UnitTests/Plans/Query/QueryPlanBuilderTest.cpp")


# Lambda Source Integration tests
add_nes_integration_test(lambda-source-integration-test "Integration/Source/LambdaSourceIntegrationTest.cpp")


### Pattern Query Plan Tests ###
add_executable(pattern-query-plan-tests "UnitTests/Services/PatternParsingServiceTest.cpp")
target_link_libraries(pattern-query-plan-tests nes-tests)
add_test(NAME pattern-query-plan-tests COMMAND pattern-query-plan-tests)

# SimplePatternTest
add_nes_integration_test(simple-pattern-test "Integration/SimplePatternTest.cpp")


### OrOperatorTest ###
add_nes_integration_test(or-operator-test "Integration/OrOperatorTest.cpp")


### AndOperatorTest ###
add_nes_integration_test(and-operator-test "Integration/AndOperatorTest.cpp")


### AndOperatorTest ###
add_nes_integration_test(seq-operator-test "Integration/SEQOperatorTest.cpp")


### Sensor Bus Tests ###
#add_nes_unit_test(sensor-bus-tests "UnitTests/SensorBusTest.cpp")
#
#

### Global Query Node Tests ###
add_nes_unit_test(global-query-node-tests "UnitTests/Plans/Global/Query/GlobalQueryNodeTest.cpp")


### Global Query Plan Tests ###
add_nes_unit_test(global-query-plan-tests "UnitTests/Plans/Global/Query/GlobalQueryPlanTest.cpp")


### Choose MemLayout Phase Tests ###
add_nes_unit_test(memory-layout-selection-phase-tests "UnitTests/Optimizer/Phases/MemoryLayoutSelectionPhaseTest.cpp")


### Query Merger Phase Tests ###
add_nes_unit_test(global-query-plan-update-phase-tests "UnitTests/Optimizer/Phases/GlobalQueryPlanUpdatePhaseTest.cpp")


### Logical Source Expansion Rule Tests ###
add_nes_unit_test(logical-source-expansion-rule-test "UnitTests/Optimizer/QueryRewrite/LogicalSourceExpansionRuleTest.cpp")


### Syntax Based Equal Query Merger Rule Test ###
add_nes_unit_test(syntax-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedCompleteQueryMergerRuleTest.cpp")


### Syntax Based Partial Query Merger Rule Test ###
add_nes_unit_test(syntax-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/SyntaxBasedPartialQueryMergerRuleTest.cpp")


### Query Placement Tests ###
add_nes_unit_test(query-placement-test "UnitTests/Optimizer/QueryPlacement/QueryPlacementTest.cpp")

add_nes_unit_test(ilp-placement-test "UnitTests/Optimizer/QueryPlacement/ILPPlacementTest.cpp")

if (NES_USE_TF)
    add_nes_unit_test(ml-heuristic-placement-test "UnitTests/Optimizer/QueryPlacement/MlHeuristicPlacementTest.cpp")
endif ()

add_nes_unit_test(nemo-placement-test "UnitTests/Optimizer/QueryPlacement/NemoPlacementTest.cpp")
#add_executable(nemo-placement-test "UnitTests/Optimizer/QueryPlacement/NemoPlacementTest.cpp")
#target_link_libraries(nemo-placement-test nes-tests)
#add_test(NAME nemo-placement-test COMMAND nemo-placement-test)

### Thread Naming Test ###
add_nes_unit_test(global-execution-plan-tests "UnitTests/Plans/Global/Execution/GlobalExecutionPlanTest.cpp")


### Util Function Test ###
add_nes_unit_test(util-function-tests "UnitTests/Util/UtilFunctionTest.cpp")


### Z3 Validation Test ###
add_nes_unit_test(z3-validation-tests "UnitTests/Z3ValidationTest.cpp")


### Syntactic Query Validation Test ###
add_nes_unit_test(syntactic-query-validation-test "UnitTests/Optimizer/QueryValidation/SyntacticQueryValidationTest.cpp")


### Semantic Query Validation Test ###
add_nes_unit_test(semantic-query-validation-test "UnitTests/Optimizer/QueryValidation/SemanticQueryValidationTest.cpp")
#target_link_libraries(semantic-query-validation-test nes-tests)
#add_test(NAME semantic-query-validation-test COMMAND semantic-query-validation-test)

### Operator To FOL Util Test ###
add_nes_unit_test(query-signature-util-tests "UnitTests/Optimizer/QuerySignatures/QuerySignatureUtilTests.cpp")


### Z3 Signature Inference Phase Test ###
add_nes_unit_test(z3-signature-inference-phase-test "UnitTests/Optimizer/Phases/SignatureInferencePhaseTest.cpp")

### Circular Buffer Tests ###
add_nes_unit_test(circular-buffer-tests "UnitTests/Source/CircularBufferTest.cpp")


### Z3 Signature Based Equal Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRuleTest.cpp")


### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRuleTest.cpp")


### Z3 Signature Based Partial Query Merger Rule Test ###
add_nes_unit_test(z3-signature-based-partial-query-merger-bottom-up-rule-test "UnitTests/Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerBottomUpRuleTest.cpp")


### String Signature Based Complete Query Merger Rule Test ###
add_nes_unit_test(hash-signature-based-equal-query-merger-rule-test "UnitTests/Optimizer/QueryMerger/HashSignatureBasedCompleteQueryMergerRuleTest.cpp")

### Join Handler Tests ###
add_nes_unit_test(join-handler-tests "UnitTests/JoinHandlerTest.cpp")


### Attribute Sort Rule Test ###
add_nes_unit_test(attribute-sort-rule-test "UnitTests/Optimizer/QueryRewrite/AttributeSortRuleTest.cpp")


### Rename Source To Project Operator Rule Test ###
add_nes_unit_test(rename-source-to-project-operator-rule-test "UnitTests/Optimizer/QueryRewrite/RenameSourceToProjectOperatorRuleTest.cpp")


### Project Before Union Operator Rule Test ###
add_nes_unit_test(project-before-union-operator-rule-test "UnitTests/Optimizer/QueryRewrite/ProjectBeforeUnionOperatorRuleTest.cpp")


### Millisecond Interval Tests ###
add_nes_unit_test(millisecond-interval-tests "UnitTests/Source/MillisecondIntervalTest.cpp")


### Materialized View Tests ###
add_nes_unit_test(materialized-view-tests "Integration/MaterializedViewTest.cpp")


### Binary Operator Sort Rule Tests ###
add_nes_unit_test(binary-operator-sort-rule-tests "UnitTests/Optimizer/QueryRewrite/BinaryOperatorSortRuleTest.cpp")


### Config Tests ###
add_nes_unit_test(config-test "UnitTests/Configurations/ConfigTest.cpp")


### Timer Tests ###
add_nes_unit_test(timer-test "UnitTests/Util/TimerTest.cpp")


### Adaptive Source Tests ###
add_nes_unit_test(adaptive-sensor-tests "UnitTests/Sensors/AdaptiveKFTest.cpp")


### Location Tests ###
add_nes_unit_test(location-unit-tests "UnitTests/Common/LocationUnitTest.cpp")


### Async Task Executor ###
add_nes_unit_test(async-task-executor-ut "UnitTests/Runtime/AsyncTaskExecutorTest.cpp")

### Location Provider CSV Tests ###
add_nes_unit_test(location-provider-csv-unit-tests "UnitTests/Spatial/LocationProviderCSVTest.cpp")


### Trajectory Predictor Test ###
add_nes_unit_test(trajectory-predictor-tests "UnitTests/Spatial/TrajectoryPredictorTest.cpp")
#------------------------------- # Integration Test Suite --------------------------------
### MultiWorkerTest ###
add_nes_integration_test(multi-worker-test "Integration/MultiWorkerTest.cpp")


### QueryDeploymentTest ###
add_nes_integration_test(query-deployment-test "Integration/QueryDeploymentTest.cpp")


### MQTTSinkDepleymentTest ###
add_nes_integration_test(mqtt-sink-deployment-test "Integration/MQTTSinkDeploymentTest.cpp")


### RenameTest ###
add_nes_integration_test(rename-test "Integration/RenameTest.cpp")


### JoinDeploymentTest ###
add_nes_integration_test(join-deployment-test "Integration/JoinDeploymentTest.cpp")

### windowDeploymentTest ###
add_nes_integration_test(window-deployment-test "Integration/WindowDeploymentTest.cpp")


### windowDeploymentTest ###
add_nes_integration_test(thread-local-tumbling-window-tests "Integration/Windowing/TumblingWindow/ThreadLocalTumblingWindowTests.cpp")


add_nes_integration_test(thread-local-global-tumbling-window-tests "Integration/Windowing/TumblingWindow/ThreadLocalGlobalTumblingWindowTests.cpp")


add_nes_integration_test(thread-local-sliding-window-tests "Integration/Windowing/SlidingWindow/ThreadLocalKeyedSlidingWindowTests.cpp")


add_nes_integration_test(thread-local-global-sliding-window-tests "Integration/Windowing/SlidingWindow/ThreadLocalGlobalSlidingWindowTests.cpp")


### concurrent window deployment test ###
add_nes_integration_test(concurrent-window-deployment-test "Integration/ConcurrentWindowDeploymentTest.cpp")


### MultipleWindowsTest ###
add_nes_integration_test(multi-window-test "Integration/MultipleWindowsTest.cpp")


### MultiJoinsTest ###
add_nes_integration_test(multi-join-test "Integration/MultipleJoinsTest.cpp")


### MultiThreadedTest ###
add_nes_integration_test(multi-thread-test "Integration/MultiThreadedTest.cpp")


### AssignWatermarkTest ###
#TODO: activate for testing
add_nes_integration_test(assignment-watermark-test "Integration/AssignWatermarkTest.cpp")


add_nes_integration_test(allowed-lateness-test "Integration/AllowedLatenessTest.cpp")


add_nes_integration_test(complex-sequence-test "Integration/ComplexSequenceTest.cpp")


### unionDeploymentTest ###
add_nes_integration_test(union-deployment-test "Integration/UnionDeploymentTest.cpp")

# TCPSourceIntegrationTest
add_nes_integration_test(tcp-source-integration-test "Integration/Source/TCPSourceIntegrationTest.cpp")
set_tests_properties(tcp-source-integration-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(tcp-source-integration-test nesCoordinator nesWorker)

### ContiniousSourceTest ###
add_nes_integration_test(continuous-source-test "Integration/Source/ContinuousSourceTest.cpp")

if (NES_USE_TF)
### MLModelDeploymentTest ###
add_nes_integration_test(ml-model-deployment-test "Integration/Operators/MLModelDeploymentTest.cpp")
endif()

### DeepHierarchyTopologyTestDeepHierarchyTopologyTest ###
add_nes_integration_test(deep-hierarchy-test "Integration/DeepHierarchyTopologyTest.cpp")


### Catalog Remote Tests ###
add_nes_integration_test(source-catalog-remote-test "Integration/Source/SourceCatalogRemoteTest.cpp")


### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(worker-coordinator-starter-tests "Integration/WorkerCoordinatorStarterTest.cpp")


# RESTEndpointTest
add_nes_integration_test(rest-endpoint-test "Integration/RESTEndpointTest.cpp")


### WorkerCoordinatorStarter Tests ###
add_nes_integration_test(update-topology-remote-test "Integration/UpdateTopologyRemoteTest.cpp")


# NetworkStackUnitTest
add_nes_integration_test(network-stack-test "UnitTests/Network/NetworkStackTest.cpp")


# NetworkStackIntegrationTest
add_nes_integration_test(network-stack-integration-test "Integration/NetworkStackIntegrationTest.cpp")


# ResourcesReaderTest
add_nes_unit_test(resources-reader-test "UnitTests/Monitoring/ResourcesReaderTest.cpp")


# MonitoringQueriesTest
add_nes_integration_test(monitoring-queries-test "UnitTests/Monitoring/MonitoringQueriesTest.cpp")


# MetricCollectorTest
add_nes_unit_test(metric-collector-test "UnitTests/Monitoring/MetricCollectorTest.cpp")


# MetricStoreTest
add_nes_unit_test(metric-store-test "UnitTests/Monitoring/MetricStoreTest.cpp")


# UserDefinedMonitoringTest
add_nes_unit_test(user-defined-monitoring-test "UnitTests/Monitoring/UserDefinedMonitoringTest.cpp")


# MonitoringIntegrationTest
add_nes_integration_test(monitoring-integration-test "Integration/MonitoringIntegrationTest.cpp")
set_tests_properties(monitoring-integration-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(monitoring-integration-test nesCoordinator nesWorker)

# MonitoringQueriesIntegrationTest
add_nes_integration_test(monitoring-queries-integration-test "Integration/MonitoringQueriesIntegrationTest.cpp")
set_tests_properties(monitoring-queries-integration-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(monitoring-queries-integration-test nesCoordinator nesWorker)

# NemoIntegrationTest
add_nes_integration_test(nemo-integration-test "Integration/NemoIntegrationTest.cpp")
add_dependencies(nemo-integration-test nesCoordinator nesWorker)

### MemorySourceIntegrationTest ###
add_nes_integration_test(memory-source-integration-test "Integration/Source/MemorySourceIntegrationTest.cpp")

### VariableLengthIntegrationTest ###
add_nes_integration_test(variable-length-integration-test "Integration/Source/VariableLengthIntegrationTest.cpp")


### StaticDataSourceIntegrationTest ###
add_nes_integration_test(static-data-source-integration-test "Integration/StaticDataSourceIntegrationTest.cpp")


### BenchmarkSourceIntegrationTest ###
add_nes_integration_test(benchmark-source-integration-test "Integration/Source/BenchmarkSourceIntegrationTest.cpp")


### TestHarnessUtilTest ###
add_nes_integration_test(test-harness-util-test "Integration/TestHarnessUtilTest.cpp")


### StringQueryTest ###
add_nes_integration_test(string-query-test "Integration/StringQueryTest.cpp")


### UpstreamBackup ###
add_nes_integration_test(upstream-backup-test "Integration/UpstreamBackupTest.cpp")

### gRPC Tests
add_nes_integration_test(grpc-test "UnitTests/gRPC/grpcTests.cpp")

### Query Failure Test
add_nes_integration_test(query-failure-test "Integration/QueryFailureTest.cpp")

### NES Coordinator Test
add_executable(nes-coordinator-test "UnitTests/Components/NesCoordinatorTest.cpp")
target_link_libraries(nes-coordinator-test nes-tests)
add_test(NAME nes-coordinator-test COMMAND nes-coordinator-test)

### Location Integration Test ###
if (NES_USE_S2)
    add_nes_integration_test(location-integration-tests "Integration/LocationIntegrationTests.cpp")
    target_link_libraries(location-integration-tests nes-tests)
endif ()

add_subdirectory(UnitTests/QueryExecution)

#------------------------------- # E2E Test Suite --------------------------------
### E2E Tests ###
add_executable(e2e-coordinator-single-worker-test "E2e/E2ECoordinatorSingleWorkerTest.cpp")
target_link_libraries(e2e-coordinator-single-worker-test nes-tests)
add_test(NAME e2e-coordinator-single-worker-test COMMAND e2e-coordinator-single-worker-test)
set_tests_properties(e2e-coordinator-single-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-single-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-worker-test "E2e/E2ECoordinatorMultiWorkerTest.cpp")
target_link_libraries(e2e-coordinator-multi-worker-test nes-tests)
add_test(NAME e2e-coordinator-multi-worker-test COMMAND e2e-coordinator-multi-worker-test)
set_tests_properties(e2e-coordinator-multi-worker-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-worker-test nesCoordinator nesWorker)

add_executable(e2e-coordinator-multi-query-test "E2e/E2ECoordinatorMultiQueryTest.cpp")
target_link_libraries(e2e-coordinator-multi-query-test nes-tests)
add_test(NAME e2e-coordinator-multi-query-test COMMAND e2e-coordinator-multi-query-test)
set_tests_properties(e2e-coordinator-multi-query-test PROPERTIES WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
add_dependencies(e2e-coordinator-multi-query-test nesCoordinator nesWorker)

set_tests_properties(${Tests} PROPERTIES TIMEOUT 60)



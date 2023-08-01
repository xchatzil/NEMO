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
#include <API/Schema.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/MemoryProvider/RowMemoryProvider.hpp>
#include <Execution/Operators/Scan.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinSink.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <gtest/gtest.h>
#include <iostream>
#include <string>

namespace NES::Runtime::Execution {

class StreamJoinMockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    StreamJoinMockedPipelineExecutionContext(BufferManagerPtr bufferManager,
                                             uint64_t noWorkerThreads,
                                             OperatorHandlerPtr streamJoinOpHandler,
                                             uint64_t pipelineId)
        : PipelineExecutionContext(
            pipelineId,// mock pipeline id
            1,         // mock query id
            bufferManager,
            noWorkerThreads,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                this->emittedBuffers.emplace_back(std::move(buffer));
            },
            {streamJoinOpHandler}){};

    std::vector<Runtime::TupleBuffer> emittedBuffers;
};

class StreamJoinPipelineTest : public Testing::NESBaseTest, public AbstractPipelineExecutionTest {

  public:
    ExecutablePipelineProvider* provider;
    BufferManagerPtr bufferManager;
    WorkerContextPtr workerContext;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinPipelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StreamJoinPipelineTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup StreamJoinPipelineTest test case.");
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bufferManager = std::make_shared<Runtime::BufferManager>();
        workerContext = std::make_shared<WorkerContext>(0, bufferManager, 100);
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StreamJoinPipelineTest test class."); }
};

void buildLeftAndRightHashTable(std::vector<TupleBuffer>& allBuffersLeft,
                                std::vector<TupleBuffer>& allBuffersRight,
                                BufferManagerPtr bufferManager,
                                SchemaPtr leftSchema,
                                SchemaPtr rightSchema,
                                ExecutablePipelineStage* executablePipelineLeft,
                                ExecutablePipelineStage* executablePipelineRight,
                                PipelineExecutionContext& pipelineExecCtxLeft,
                                PipelineExecutionContext& pipelineExecCtxRight,
                                Runtime::MemoryLayouts::RowLayoutPtr memoryLayoutLeft,
                                Runtime::MemoryLayouts::RowLayoutPtr memoryLayoutRight,
                                WorkerContextPtr workerContext,
                                size_t numberOfTuplesToProduce) {

    auto tuplePerBufferLeft = bufferManager->getBufferSize() / leftSchema->getSchemaSizeInBytes();
    auto tuplePerBufferRight = bufferManager->getBufferSize() / rightSchema->getSchemaSizeInBytes();

    auto bufferLeft = bufferManager->getBufferBlocking();
    auto bufferRight = bufferManager->getBufferBlocking();
    for (auto i = 0UL; i < numberOfTuplesToProduce + 1; ++i) {
        if (bufferLeft.getNumberOfTuples() >= tuplePerBufferLeft) {
            executablePipelineLeft->execute(bufferLeft, pipelineExecCtxLeft, *workerContext);
            allBuffersLeft.emplace_back(bufferLeft);
            bufferLeft = bufferManager->getBufferBlocking();
        }

        auto dynamicBufferLeft = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutLeft, bufferLeft);
        auto posLeft = dynamicBufferLeft.getNumberOfTuples();
        dynamicBufferLeft[posLeft][leftSchema->get(0)->getName()].write(i + 1000);
        dynamicBufferLeft[posLeft][leftSchema->get(1)->getName()].write((i % 10) + 10);
        dynamicBufferLeft[posLeft][leftSchema->get(2)->getName()].write(i);
        bufferLeft.setNumberOfTuples(posLeft + 1);

        if (bufferRight.getNumberOfTuples() >= tuplePerBufferRight) {
            executablePipelineRight->execute(bufferRight, pipelineExecCtxRight, *workerContext);
            allBuffersRight.emplace_back(bufferRight);
            bufferRight = bufferManager->getBufferBlocking();
        }

        auto dynamicBufferRight = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutRight, bufferRight);
        auto posRight = dynamicBufferRight.getNumberOfTuples();
        dynamicBufferRight[posRight][rightSchema->get(0)->getName()].write(i + 2000);
        dynamicBufferRight[posRight][rightSchema->get(1)->getName()].write((i % 10) + 10);
        dynamicBufferRight[posRight][rightSchema->get(2)->getName()].write(i);
        bufferRight.setNumberOfTuples(posRight + 1);
    }
    if (bufferLeft.getNumberOfTuples() > 0) {
        executablePipelineLeft->execute(bufferLeft, pipelineExecCtxLeft, *workerContext);
        allBuffersLeft.emplace_back(bufferLeft);
    }

    if (bufferRight.getNumberOfTuples() > 0) {
        executablePipelineRight->execute(bufferRight, pipelineExecCtxRight, *workerContext);
        allBuffersRight.push_back(bufferRight);
    }
}

void performNLJ(std::vector<TupleBuffer>& nljBuffers,
                std::vector<TupleBuffer>& allBuffersLeft,
                std::vector<TupleBuffer>& allBuffersRight,
                size_t windowSize,
                const std::string& timeStampField,
                Runtime::MemoryLayouts::RowLayoutPtr memoryLayoutLeft,
                Runtime::MemoryLayouts::RowLayoutPtr memoryLayoutRight,
                Runtime::MemoryLayouts::RowLayoutPtr memoryLayoutJoined,
                const std::string& joinFieldNameLeft,
                const std::string& joinFieldNameRight,
                BufferManagerPtr bufferManager) {

    uint64_t lastTupleTimeStampWindow = windowSize - 1;
    uint64_t firstTupleTimeStampWindow = 0;
    auto bufferJoined = bufferManager->getBufferBlocking();
    for (auto bufLeft : allBuffersLeft) {
        auto dynamicBufLeft = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutLeft, bufLeft);
        for (auto tupleLeftCnt = 0UL; tupleLeftCnt < dynamicBufLeft.getNumberOfTuples(); ++tupleLeftCnt) {

            auto timeStampLeft = dynamicBufLeft[tupleLeftCnt][timeStampField].read<uint64_t>();
            if (timeStampLeft > lastTupleTimeStampWindow) {
                lastTupleTimeStampWindow += windowSize;
                firstTupleTimeStampWindow += windowSize;

                nljBuffers.emplace_back(bufferJoined);
                bufferJoined = bufferManager->getBufferBlocking();
            }

            for (auto bufRight : allBuffersRight) {
                auto dynamicBufRight = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutRight, bufRight);
                for (auto tupleRightCnt = 0UL; tupleRightCnt < dynamicBufRight.getNumberOfTuples(); ++tupleRightCnt) {
                    auto timeStampRight = dynamicBufRight[tupleRightCnt][timeStampField].read<uint64_t>();
                    if (timeStampRight > lastTupleTimeStampWindow || timeStampRight < firstTupleTimeStampWindow) {
                        continue;
                    }

                    auto leftKey = dynamicBufLeft[tupleLeftCnt][joinFieldNameLeft].read<uint64_t>();
                    auto rightKey = dynamicBufRight[tupleRightCnt][joinFieldNameRight].read<uint64_t>();

                    if (leftKey == rightKey) {
                        auto dynamicBufJoined = Runtime::MemoryLayouts::DynamicTupleBuffer(memoryLayoutJoined, bufferJoined);
                        auto posNewTuple = dynamicBufJoined.getNumberOfTuples();
                        dynamicBufJoined[posNewTuple][0].write<uint64_t>(leftKey);

                        dynamicBufJoined[posNewTuple][1].write<uint64_t>(dynamicBufLeft[tupleLeftCnt][0].read<uint64_t>());
                        dynamicBufJoined[posNewTuple][2].write<uint64_t>(dynamicBufLeft[tupleLeftCnt][1].read<uint64_t>());
                        dynamicBufJoined[posNewTuple][3].write<uint64_t>(dynamicBufLeft[tupleLeftCnt][2].read<uint64_t>());

                        dynamicBufJoined[posNewTuple][4].write<uint64_t>(dynamicBufRight[tupleRightCnt][0].read<uint64_t>());
                        dynamicBufJoined[posNewTuple][5].write<uint64_t>(dynamicBufRight[tupleRightCnt][1].read<uint64_t>());
                        dynamicBufJoined[posNewTuple][6].write<uint64_t>(dynamicBufRight[tupleRightCnt][2].read<uint64_t>());

                        dynamicBufJoined.setNumberOfTuples(posNewTuple + 1);
                        if (dynamicBufJoined.getNumberOfTuples() >= dynamicBufJoined.getCapacity()) {
                            nljBuffers.emplace_back(bufferJoined);
                            bufferJoined = bufferManager->getBufferBlocking();
                        }
                    }
                }
            }
        }
    }
}

TEST_P(StreamJoinPipelineTest, streamJoinPipeline) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);
    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);
    const auto joinBuildEmitSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                         ->addField("partitionId", BasicType::UINT64)
                                         ->addField("lastTupleTimeStamp", BasicType::UINT64);

    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();

    EXPECT_EQ(leftSchema->getLayoutType(), rightSchema->getLayoutType());
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

    EXPECT_EQ(leftSchema->get(2)->getName(), rightSchema->get(2)->getName());
    auto timeStampField = leftSchema->get(2)->getName();

    auto memoryLayoutLeft = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bufferManager->getBufferSize());
    auto memoryLayoutRight = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bufferManager->getBufferSize());
    auto memoryLayoutJoined = Runtime::MemoryLayouts::RowLayout::create(joinSchema, bufferManager->getBufferSize());

    auto scanMemoryProviderLeft = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutLeft);
    auto scanMemoryProviderRight = std::make_unique<MemoryProvider::RowMemoryProvider>(memoryLayoutRight);

    auto scanOperatorLeft = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderLeft));
    auto scanOperatorRight = std::make_shared<Operators::Scan>(std::move(scanMemoryProviderRight));

    auto noWorkerThreads = 1;
    auto numSourcesLeft = 1, numSourcesRight = 1;
    auto joinSizeInByte = 1 * 1024 * 1024;
    auto windowSize = 20UL;
    auto numberOfTuplesToProduce = windowSize * 20;

    auto handlerIndex = 0;
    auto joinBuildLeft = std::make_shared<Operators::StreamJoinBuild>(handlerIndex,
                                                                      /*isLeftSide*/ true,
                                                                      joinFieldNameLeft,
                                                                      timeStampField,
                                                                      leftSchema);
    auto joinBuildRight = std::make_shared<Operators::StreamJoinBuild>(handlerIndex,
                                                                       /*isLeftSide*/ false,
                                                                       joinFieldNameRight,
                                                                       timeStampField,
                                                                       rightSchema);
    auto joinSink = std::make_shared<Operators::StreamJoinSink>(handlerIndex);
    auto streamJoinOpHandler = std::make_shared<Operators::StreamJoinOperatorHandler>(leftSchema,
                                                                                      rightSchema,
                                                                                      joinFieldNameLeft,
                                                                                      joinFieldNameRight,
                                                                                      noWorkerThreads * 2,
                                                                                      numSourcesLeft + numSourcesRight,
                                                                                      joinSizeInByte,
                                                                                      windowSize);

    scanOperatorLeft->setChild(joinBuildLeft);
    scanOperatorRight->setChild(joinBuildRight);

    auto pipelineBuildLeft = std::make_shared<PhysicalOperatorPipeline>();
    auto pipelineBuildRight = std::make_shared<PhysicalOperatorPipeline>();
    auto pipelineSink = std::make_shared<PhysicalOperatorPipeline>();
    pipelineBuildLeft->setRootOperator(scanOperatorLeft);
    pipelineBuildRight->setRootOperator(scanOperatorRight);
    pipelineSink->setRootOperator(joinSink);

    auto curPipelineId = 0;
    auto pipelineExecCtxLeft =
        StreamJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, streamJoinOpHandler, curPipelineId++);
    auto pipelineExecCtxRight =
        StreamJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, streamJoinOpHandler, curPipelineId++);
    auto pipelineExecCtxSink =
        StreamJoinMockedPipelineExecutionContext(bufferManager, noWorkerThreads, streamJoinOpHandler, curPipelineId++);

    auto executablePipelineLeft = provider->create(pipelineBuildLeft);
    auto executablePipelineRight = provider->create(pipelineBuildRight);
    auto executablePipelineSink = provider->create(pipelineSink);

    EXPECT_EQ(executablePipelineLeft->setup(pipelineExecCtxLeft), 0);
    EXPECT_EQ(executablePipelineRight->setup(pipelineExecCtxRight), 0);
    EXPECT_EQ(executablePipelineSink->setup(pipelineExecCtxSink), 0);

    // Filling left and right hash tables
    std::vector<Runtime::TupleBuffer> allBuffersLeft, allBuffersRight;
    buildLeftAndRightHashTable(allBuffersLeft,
                               allBuffersRight,
                               bufferManager,
                               leftSchema,
                               rightSchema,
                               executablePipelineLeft.get(),
                               executablePipelineRight.get(),
                               pipelineExecCtxLeft,
                               pipelineExecCtxRight,
                               memoryLayoutLeft,
                               memoryLayoutRight,
                               workerContext,
                               numberOfTuplesToProduce);

    // Assure that at least one buffer has been emitted
    EXPECT_TRUE(pipelineExecCtxLeft.emittedBuffers.size() > 0 || pipelineExecCtxRight.emittedBuffers.size() > 0);

    // Calling join Sink
    std::vector<Runtime::TupleBuffer> buildEmittedBuffers(pipelineExecCtxLeft.emittedBuffers);
    buildEmittedBuffers.insert(buildEmittedBuffers.end(),
                               pipelineExecCtxRight.emittedBuffers.begin(),
                               pipelineExecCtxRight.emittedBuffers.end());

    NES_DEBUG("Calling joinSink for " << buildEmittedBuffers.size() << " buffers");
    for (auto buf : buildEmittedBuffers) {
        executablePipelineSink->execute(buf, pipelineExecCtxSink, *workerContext);
    }

    std::vector<Runtime::TupleBuffer> nljBuffers;
    performNLJ(nljBuffers,
               allBuffersLeft,
               allBuffersRight,
               windowSize,
               timeStampField,
               memoryLayoutLeft,
               memoryLayoutRight,
               memoryLayoutJoined,
               joinFieldNameLeft,
               joinFieldNameRight,
               bufferManager);

    auto mergedEmittedBuffers =
        Util::mergeBuffersSameWindow(pipelineExecCtxSink.emittedBuffers, joinSchema, timeStampField, bufferManager, windowSize);

    // We have to sort and merge the emitted buffers as otherwise we can not simply compare versus a NLJ version
    auto sortedMergedEmittedBuffers =
        Util::sortBuffersInTupleBuffer(mergedEmittedBuffers, joinSchema, timeStampField, bufferManager);
    auto sortNLJBuffers = Util::sortBuffersInTupleBuffer(nljBuffers, joinSchema, timeStampField, bufferManager);

    pipelineExecCtxSink.emittedBuffers.clear();
    mergedEmittedBuffers.clear();
    nljBuffers.clear();

    EXPECT_EQ(sortNLJBuffers.size(), sortedMergedEmittedBuffers.size());
    for (auto i = 0UL; i < sortNLJBuffers.size(); ++i) {
        auto nljBuffer = sortNLJBuffers[i];
        auto streamJoinBuf = sortedMergedEmittedBuffers[i];

        NES_DEBUG("Comparing nljBuffer\n"
                  << Util::printTupleBufferAsCSV(nljBuffer, joinSchema) << "\n and streamJoinBuf\n"
                  << Util::printTupleBufferAsCSV(streamJoinBuf, joinSchema));

        EXPECT_EQ(nljBuffer.getNumberOfTuples(), streamJoinBuf.getNumberOfTuples());
        EXPECT_EQ(nljBuffer.getBufferSize(), streamJoinBuf.getBufferSize());
        EXPECT_TRUE(memcmp(nljBuffer.getBuffer(), streamJoinBuf.getBuffer(), streamJoinBuf.getBufferSize()) == 0);
    }

    // Stopping all executable pipelines
    EXPECT_EQ(executablePipelineLeft->stop(pipelineExecCtxLeft), 0);
    EXPECT_EQ(executablePipelineLeft->stop(pipelineExecCtxRight), 0);
    EXPECT_EQ(executablePipelineSink->stop(pipelineExecCtxSink), 0);
}

INSTANTIATE_TEST_CASE_P(testIfCompilation,
                        StreamJoinPipelineTest,
                        ::testing::Values("PipelineInterpreter", "PipelineCompiler"),
                        [](const testing::TestParamInfo<StreamJoinPipelineTest::ParamType>& info) {
                            return info.param;
                        });

}// namespace NES::Runtime::Execution
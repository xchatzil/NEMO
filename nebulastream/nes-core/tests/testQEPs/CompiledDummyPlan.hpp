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

/*
 * compiledTestPlan.hpp
 *
 *  Created on: Feb 19, 2019
 *      Author: zeuchste
 */

#ifndef NES_TESTS_TEST_QEPS_COMPILED_DUMMY_PLAN_HPP_
#define NES_TESTS_TEST_QEPS_COMPILED_DUMMY_PLAN_HPP_

#include <API/InputQuery.hpp>
#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <Runtime/QueryManager.hpp>
#include <Sources/GeneratorSource.hpp>
#include <memory>

namespace NES {
class TupleBuffer;
class CompiledDummyPlan : public HandCodedQueryExecutionPlan {
  public:
    uint64_t count;
    uint64_t sum;
    CompiledDummyPlan() : HandCodedQueryExecutionPlan(), count(0), sum(0) {}

    bool firstPipelineStage(const TupleBuffer&) { return false; }

    bool executeStage(uint32_t pipeline_stage_id, TupleBuffer& buf) { return true; }
};
typedef std::shared_ptr<CompiledDummyPlan> CompiledDummyPlanPtr;

};// namespace NES

#endif// NES_TESTS_TEST_QEPS_COMPILED_DUMMY_PLAN_HPP_

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

#include <Execution/Expressions/LogicalExpressions/EqualsExpression.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Operators/Selection.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Nautilus {

class OperatorTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("OperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup OperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES_INFO("Setup OperatorTest test case."); }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down OperatorTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down OperatorTest test class."); }
};

TEST_F(OperatorTest, FilterOperatorTest) {
    // setup operator
    auto readField1 = std::make_shared<ReadFieldExpression>("0");
    auto readField2 = std::make_shared<ReadFieldExpression>("1");
    auto equalsExpression = std::make_shared<EqualsExpression>(readField1, readField2);
    auto selection = std::make_shared<Selection>(equalsExpression);
}

}// namespace NES::Nautilus
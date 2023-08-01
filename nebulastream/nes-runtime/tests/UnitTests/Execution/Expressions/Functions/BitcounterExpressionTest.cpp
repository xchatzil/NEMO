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

#include <Execution/Expressions/Functions/BitcounterExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class BitcounterExpressionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("BitcounterExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup BitcounterExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down BitcounterExpressionTest test class."); }
};
TEST_F(BitcounterExpressionTest, divIntegers) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>((int32_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
}

TEST_F(BitcounterExpressionTest, divUIntegers) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>((uint32_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
    // UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 31));
        ASSERT_EQ(resultValue, (uint32_t) 5);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }
}

/**
* @brief If we execute the expression on a boolean it should throw an exception.
*/
TEST_F(BitcounterExpressionTest, evaluateBitCounterExpressionOnWrongType) {
    auto expression = UnaryExpressionWrapper<BitcounterExpression>();
    ASSERT_ANY_THROW(expression.eval(Value<Boolean>(true)););
}

}// namespace NES::Runtime::Execution::Expressions

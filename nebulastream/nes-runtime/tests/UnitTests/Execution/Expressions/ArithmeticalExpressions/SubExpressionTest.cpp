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
#include <Execution/Expressions/ArithmeticalExpressions/SubExpression.hpp>
#include <NesBaseTest.hpp>
#include <TestUtils/ExpressionWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <memory>

namespace NES::Runtime::Execution::Expressions {

class SubExpressionTest : public Testing::NESBaseTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SubExpressionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SubExpressionTest test class.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SubExpressionTest test class."); }
};

TEST_F(SubExpressionTest, subIntegers) {
    auto expression = BinaryExpressionWrapper<SubExpression>();

    // Int8
    {
        auto resultValue = expression.eval(Value<Int8>((int8_t) 42), Value<Int8>((int8_t) 42));
        ASSERT_EQ(resultValue, 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int8>());
    }
    // Int16
    {
        auto resultValue = expression.eval(Value<Int16>((int16_t) 42), Value<Int16>((int16_t) 42));
        ASSERT_EQ(resultValue, 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int16>());
    }// Int32
    {
        auto resultValue = expression.eval(Value<Int32>(42), Value<Int32>(42));
        ASSERT_EQ(resultValue, 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int32>());
    }
    // Int64
    {
        auto resultValue = expression.eval(Value<Int64>((int64_t) 42), Value<Int64>((int64_t) 42));
        ASSERT_EQ(resultValue, 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Int64>());
    }
}

TEST_F(SubExpressionTest, subUnsignedIntegers) {
    auto expression = BinaryExpressionWrapper<SubExpression>();

    // UInt8
    {
        auto resultValue = expression.eval(Value<UInt8>((uint8_t) 42), Value<UInt8>((uint8_t) 42));
        ASSERT_EQ(resultValue, (uint8_t) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt8>());
    }
    // UInt16
    {
        auto resultValue = expression.eval(Value<UInt16>((uint16_t) 42), Value<UInt16>((uint16_t) 42));
        ASSERT_EQ(resultValue, (uint16_t) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt16>());
    }// UInt32
    {
        auto resultValue = expression.eval(Value<UInt32>(42u), Value<UInt32>(42u));
        ASSERT_EQ(resultValue, (uint32_t) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt32>());
    }// UInt64
    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 42), Value<UInt64>((uint64_t) 42));
        ASSERT_EQ(resultValue, (uint64_t) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt64>());
    }

    {
        auto resultValue = expression.eval(Value<UInt64>((uint64_t) 42), Value<UInt64>((uint64_t) 43));
        ASSERT_EQ(resultValue, UINT64_MAX);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<UInt64>());
    }
}

TEST_F(SubExpressionTest, subFloat) {
    auto expression = BinaryExpressionWrapper<SubExpression>();
    // Float
    {
        auto resultValue = expression.eval(Value<Float>((float) 42), Value<Float>((float) 42));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Float>());
    }
    // Double
    {
        auto resultValue = expression.eval(Value<Double>((double) 42), Value<Double>((double) 42));
        ASSERT_EQ(resultValue, (float) 0);
        ASSERT_TRUE(resultValue->getTypeIdentifier()->isType<Double>());
    }
}

}// namespace NES::Runtime::Execution::Expressions
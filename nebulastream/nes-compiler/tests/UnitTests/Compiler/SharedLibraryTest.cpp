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
#include <Compiler/Util/SharedLibrary.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
namespace NES::Compiler {
class SharedLibraryTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SharedLibraryTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("SharedLibraryTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("SharedLibraryTest test class TearDownTestCase."); }
};

TEST(SharedLibraryTest, loadSharedLib) {
#ifdef __linux__
    auto sharedLib = SharedLibrary::load("libnes-compiler.so");
#elif defined(__APPLE__)
    auto sharedLib = SharedLibrary::load("libnes-compiler.dylib");
#else
#error "Unknown error"
#endif
    sharedLib.reset();
}

TEST(SharedLibraryTest, loadSharedLibERROR) { EXPECT_ANY_THROW(SharedLibrary::load("NotExisting.so")); }

TEST(SharedLibraryTest, loadSymbleERROR) {
    using FunctionType = uint64_t (*)();
#ifdef __linux__
    auto sharedLib = SharedLibrary::load("libnes-compiler.so");
#elif defined(__APPLE__)
    auto sharedLib = SharedLibrary::load("libnes-compiler.dylib");
#else
#error "Unknown error"
#endif
    EXPECT_ANY_THROW(sharedLib->getInvocableMember<FunctionType>("NotExisting"));
}

}// namespace NES::Compiler
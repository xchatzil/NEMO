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

#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>

namespace NES {
using Runtime::StateManager;
using Runtime::StateVariable;
class StateTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StateTest.log", NES::LogLevel::LOG_DEBUG);

        NES_INFO("Setup StateTest test class.");
    }
    static void TearDownTestCase() { NES_DEBUG("Tear down StateTest test class."); }
};

TEST_F(StateTest, testAddClear) {
    StateManager stateManager = StateManager(0);
    StateId stateId = {0, 0, 0};
    StateVariable<uint32_t, uint32_t> var = *stateManager.registerState<uint32_t, uint32_t>(stateId);
    auto kv = var[23];

    EXPECT_EQ(!!kv, false);

    kv.put(43ul);

    EXPECT_EQ(kv.value(), 43ul);

    kv.clear();// unexpected bahavior afterwards

    bool catched = false;
    try {
        EXPECT_NE(kv.value(), 43ul);
    } catch (std::out_of_range& e) {
        catched = true;
    }
    EXPECT_EQ(catched, true);
}

TEST_F(StateTest, testEmplaceClear) {
    StateManager stateManager = StateManager(1);
    StateId stateId = {0, 0, 1};
    StateVariable<uint32_t, uint32_t> var = *stateManager.registerState<uint32_t, uint32_t>(stateId);
    auto kv = var[23];

    EXPECT_EQ(!!kv, false);

    kv.emplace(43ul);

    EXPECT_EQ(kv.value(), 43ul);

    kv.clear();// unexpected bahavior afterwards

    bool catched = false;
    try {
        EXPECT_NE(kv.value(), 43ul);
    } catch (std::out_of_range& e) {
        catched = true;
    }
    EXPECT_EQ(catched, true);
}

TEST_F(StateTest, testMultipleAddLookup) {
    StateManager stateManager = StateManager(2);
    StateId stateId = {0, 0, 2};
    StateVariable<uint32_t, uint32_t> var = *stateManager.registerState<uint32_t, uint32_t>(stateId);

    std::unordered_map<uint32_t, uint32_t> map;

    for (uint64_t i = 0; i < 8192; i++) {

        uint32_t key = rand();
        uint32_t val = rand();

        var[key] = val;
        map[key] = val;
    }

    for (auto& it : map) {
        auto key = it.first;
        auto val = it.second;
        EXPECT_EQ(var[key].value(), val);
    }
}

TEST_F(StateTest, testMultipleAddLookupMt) {
    StateManager stateManager = StateManager(3);
    StateId stateId = {0, 0, 3};
    StateVariable<uint32_t, uint32_t> var = *stateManager.registerState<uint32_t, uint32_t>(stateId);

    std::vector<std::thread> t;

    std::mutex mutex;
    std::unordered_map<uint32_t, uint32_t> map;
    constexpr uint32_t num_threads = 4;
    constexpr uint32_t max_values = 2000000 / num_threads;

    for (uint32_t i = 0; i < num_threads; i++) {
        t.emplace_back([&var, &map, &mutex]() {
            std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
            for (uint64_t i = 0; i < max_values; i++) {

                uint32_t key = rand();
                uint32_t val = rand();

                var[key] = val;

                lock.lock();
                map[key] = val;
                lock.unlock();
            }
        });
    }

    for (auto& worker : t) {
        worker.join();
    }

    std::atomic_thread_fence(std::memory_order_seq_cst);

    for (auto& it : map) {
        auto key = it.first;
        auto val = it.second;
        EXPECT_EQ(var[key].value(), val);
    }
}

TEST_F(StateTest, testAddRangeMt) {
    StateManager stateManager = StateManager(4);
    StateId stateId = {0, 0, 4};
    StateVariable<uint32_t, uint32_t> var = *stateManager.registerState<uint32_t, uint32_t>(stateId);

    std::vector<std::thread> t;

    std::mutex mutex;
    std::unordered_map<uint32_t, uint32_t> map;
    constexpr uint32_t num_threads = 4;
    constexpr uint32_t max_values = 2000000 / num_threads;

    for (uint32_t i = 0; i < num_threads; i++) {
        t.emplace_back([&var, &map, &mutex]() {
            std::unique_lock<std::mutex> lock(mutex, std::defer_lock);
            for (uint64_t i = 0; i < max_values; i++) {

                uint32_t key = rand();
                uint32_t val = rand();

                var[key] = val;

                lock.lock();
                map[key] = val;
                lock.unlock();
            }
        });
    }

    for (auto& worker : t) {
        worker.join();
    }

    std::atomic_thread_fence(std::memory_order_seq_cst);

    {
        std::unique_lock<std::mutex> lock(mutex);
        auto rangeAll = var.rangeAll();
        for (auto& it : rangeAll) {
            auto key = it.first;
            auto val = it.second;

            EXPECT_EQ(map[key], val);
        }
    }
}

struct window_metadata {
    uint64_t start;
    uint64_t end;

    explicit window_metadata(uint64_t s, uint64_t e) : start(s), end(e) {}
};

TEST_F(StateTest, testStruct) {
    StateManager stateManager = StateManager(5);
    StateId stateId = {0, 0, 5};
    StateVariable<uint32_t, window_metadata*> var = *stateManager.registerState<uint32_t, window_metadata*>(stateId);

    for (uint64_t i = 0; i < 8192; i++) {

        uint32_t key = rand();
        uint64_t start = rand();
        uint64_t end = start + rand();
        var[key].emplace(start, end);
        auto v = var[key].value();

        EXPECT_EQ(v->start, start);
        EXPECT_EQ(v->end, end);
    }
}

}// namespace NES

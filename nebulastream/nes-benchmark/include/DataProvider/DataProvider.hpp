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

#ifndef NES_DATAPROVIDER_HPP
#define NES_DATAPROVIDER_HPP

#include <DataProvider/TupleBufferHolder.hpp>
#include <E2E/Configurations/E2EBenchmarkConfig.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/libcuckoo/cuckoohash_map.hh>
#include <cstdint>

namespace NES::Benchmark::DataProviding {

class DataProvider;
using DataProviderPtr = std::shared_ptr<DataProvider>;

class DataProvider {
  public:
    enum DataProviderMode { ZERO_COPY, MEM_COPY };

    /**
         * @brief creates a DataProvider object
         * @param id
         * @param providerMode
         */
    explicit DataProvider(uint64_t id, DataProvider::DataProviderMode providerMode);

    /**
         * @brief default destructor
         */
    virtual ~DataProvider() = default;

    /**
         * @brief provides the next buffer to the runtime by a DataProviderMode way
         * @param buffer
         * @param sourceId
         */
    void provideNextBuffer(Runtime::TupleBuffer& buffer, uint64_t sourceId);

    /**
         * @brief reads the next buffer for the given sourceId
         * @param sourceId
         * @return
         */
    virtual std::optional<Runtime::TupleBuffer> readNextBuffer(uint64_t sourceId) = 0;

    /**
         * @brief creates a data provider
         * @return
         */
    static std::shared_ptr<DataProvider> createProvider(uint64_t id,
                                                        NES::Benchmark::E2EBenchmarkConfigOverAllRuns configOverAllRuns,
                                                        std::vector<Runtime::TupleBuffer> buffers);

    /**
         * @brief starts this provider
         */
    virtual void start() = 0;

    /**
         * @brief stops this provider
         */
    virtual void stop() = 0;

  protected:
    uint64_t id;
    DataProviderMode providerMode;
    cuckoohash_map<uintptr_t, TupleBufferHolder> collector;
};
}// namespace NES::Benchmark::DataProviding

#endif//NES_DATAPROVIDER_HPP

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

#ifndef NES_E2EBENCHMARKCONFIGPERRUN_HPP
#define NES_E2EBENCHMARKCONFIGPERRUN_HPP

#include <Configurations/ConfigurationOption.hpp>
#include <Util/yaml/Yaml.hpp>
#include <vector>

namespace NES::Benchmark {

class E2EBenchmarkConfigPerRun {

  public:
    /**
         * @brief creates a E2EBenchmarkConfigPerRun object and sets the default values
         */
    explicit E2EBenchmarkConfigPerRun();

    /**
         * @brief creates a string representation of this object
         * @return the string representation
         */
    std::string toString();

    /**
         * @brief parses and generates the config for the parameters changing per run
         * runs by parsing the yamlConfig
         * @param yamlConfig
         * @return
         */
    static std::vector<E2EBenchmarkConfigPerRun> generateAllConfigsPerRun(Yaml::Node yamlConfig);

    Configurations::IntConfigOption numWorkerOfThreads;
    Configurations::IntConfigOption numberOfSources;
    Configurations::IntConfigOption bufferSizeInBytes;
    Configurations::IntConfigOption numberOfBuffersInGlobalBufferManager;
    Configurations::IntConfigOption numberOfBuffersPerPipeline;
    Configurations::IntConfigOption numberOfBuffersInSourceLocalBufferPool;
    Configurations::IntConfigOption numberOfQueriesToDeploy;
};
}// namespace NES::Benchmark

#endif//NES_E2EBENCHMARKCONFIGPERRUN_HPP

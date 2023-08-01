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

#ifndef NES_INCLUDE_DATAGENERATORS_YSBDATAGENERATOR_HPP_
#define NES_INCLUDE_DATAGENERATORS_YSBDATAGENERATOR_HPP_
#include <DataGeneration/DataGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

class YSBDataGenerator : public DataGenerator {
  public:
    std::string getName() override;

    std::vector<Runtime::TupleBuffer> createData(size_t numberOfBuffers, size_t bufferSize) override;

    SchemaPtr getSchema() override;

    std::string toString() override;
};

}// namespace NES::Benchmark::DataGeneration

#endif// NES_INCLUDE_DATAGENERATORS_LIGHTSABER_YSBDATAGENERATOR_HPP_

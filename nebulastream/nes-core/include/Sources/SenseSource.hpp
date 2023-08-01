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

#ifndef NES_CORE_INCLUDE_SOURCES_SENSESOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_SENSESOURCE_HPP_

#include <Sources/DataSource.hpp>
#include <fstream>
#include <string>

namespace NES {
/**
 * @brief this class implement the CSV as an input source
 */
class SenseSource : public DataSource {
  public:
    /**
   * @brief constructor of sense sou1rce
   * @param schema of the source
   * @param udfs to apply
   */
    explicit SenseSource(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         std::string udfs,
                         OperatorId operatorId,
                         OriginId originId,
                         size_t numSourceLocalBuffers,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
   * @brief override the receiveData method for the source
   * @return returns a buffer if available
   */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
   *  @brief method to fill the buffer with tuples
   *  @param buffer to be filled
   */
    void fillBuffer(Runtime::TupleBuffer&);

    /**
     * @brief override the toString method for the csv source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     * @brief Get source type
     */
    SourceType getType() const override;

    /**
     * @brief Get UDFs for sense
     */
    const std::string& getUdfs() const;

  private:
    std::string udfs;
};

using SenseSourcePtr = std::shared_ptr<SenseSource>;

}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_SENSESOURCE_HPP_

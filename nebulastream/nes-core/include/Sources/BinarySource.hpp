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

#ifndef NES_CORE_INCLUDE_SOURCES_BINARYSOURCE_HPP_
#define NES_CORE_INCLUDE_SOURCES_BINARYSOURCE_HPP_

#include <Sources/DataSource.hpp>
#include <fstream>

namespace NES {

/**
 * @brief this class provides a binary file as source
 */
class BinarySource : public DataSource {
  public:
    /**
     * @brief constructor for binary source
     * @param schema of the data source
     * @param file path
     */
    explicit BinarySource(const SchemaPtr& schema,
                          Runtime::BufferManagerPtr bufferManager,
                          Runtime::QueryManagerPtr queryManager,
                          const std::string& file_path,
                          OperatorId operatorId,
                          OriginId originId,
                          size_t numSourceLocalBuffers,
                          GatheringMode::Value gatheringMode,
                          std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);

    /**
     * @brief override the receiveData method for the binary source
     * @return returns a buffer if available
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    std::string toString() const override;

    /**
     *  @brief method to fill the buffer with tuples
     *  @param buffer to be filled
     */
    void fillBuffer(Runtime::TupleBuffer&);

    SourceType getType() const override;

    const std::string& getFilePath() const;

  protected:
    std::ifstream input;

  private:
    std::string filePath;

    size_t fileSize;
    uint64_t tupleSize;
};

using BinarySourcePtr = std::shared_ptr<BinarySource>;

}// namespace NES
#endif// NES_CORE_INCLUDE_SOURCES_BINARYSOURCE_HPP_

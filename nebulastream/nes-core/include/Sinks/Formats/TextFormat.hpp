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

#ifndef NES_CORE_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_
#define NES_CORE_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class TextFormat : public SinkFormat {
  public:
    TextFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager);
    virtual ~TextFormat() noexcept = default;

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    std::vector<Runtime::TupleBuffer> getData(Runtime::TupleBuffer& inputBuffer) override;

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    FormatIterator getTupleIterator(Runtime::TupleBuffer& inputBuffer) override;

    /**
    * @brief method to write the schema of the data
    * @return TupleBuffer containing the schema
    */
    std::optional<Runtime::TupleBuffer> getSchema() override;

    /**
   * @brief method to return the format as a string
   * @return format as string
   */
    std::string toString() override;

    /**
     * @brief return sink format
     * @return sink format
     */
    FormatTypes getSinkFormat() override;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_SINKS_FORMATS_TEXTFORMAT_HPP_

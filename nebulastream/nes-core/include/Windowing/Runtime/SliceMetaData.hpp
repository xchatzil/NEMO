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

#ifndef NES_CORE_INCLUDE_WINDOWING_RUNTIME_SLICEMETADATA_HPP_
#define NES_CORE_INCLUDE_WINDOWING_RUNTIME_SLICEMETADATA_HPP_
#include <cstdint>
namespace NES::Windowing {

/**
* SliceMetaData stores the meta data of a slice to identify if a record can be assigned to a particular slice.
*/
class SliceMetaData {
  public:
    /**
     * @brief Construct a new slice meta data object.
     * @param startTs
     * @param endTs
     */
    SliceMetaData(uint64_t startTs, uint64_t endTs);

    /**
    * @brief The start timestamp of this slice
    */
    uint64_t getStartTs() const;

    /**
     * @brief The end timestamp of this slice
     */
    uint64_t getEndTs() const;

    /**
     * @brief method to get the number of tuples per slice
     * @return number of tuples per slice
     */
    uint64_t getRecordsPerSlice() const;

    /**
     * @brief method to increment the number of tuples per slice by one
     */
    void incrementRecordsPerSlice();

    /**
     * @brief method to increment the number of tuples per slice by value
     * @param value
     */
    void incrementRecordsPerSliceByValue(uint64_t value);

  private:
    uint64_t startTs;
    uint64_t endTs;
    uint64_t recordsPerSlice;
};

}// namespace NES::Windowing
#endif// NES_CORE_INCLUDE_WINDOWING_RUNTIME_SLICEMETADATA_HPP_

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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICE_HPP_
#include <Util/Experimental/HashMap.hpp>
#include <cinttypes>
#include <ostream>
namespace NES::Windowing::Experimental {

/**
 * @brief A keyed slice that contains key value pairs for a specific interval of [start, end[.
 */
class KeyedSlice {
  public:
    /**
     * @brief Constructor to create a new slice that covers a specific range between stat and end.
     * @param hashMapFactory a factory to create a new hashmap
     * @param start of the slice
     * @param end of the slice
     * @param index of the slice (currently we assume that we can calculate a slice index, to which a specific stream event is assigned).
     */
    KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory, uint64_t start, uint64_t end);

    /**
     * @brief Constructor to create a uninitialized slice.
     * @param hashMapFactory
     */
    KeyedSlice(std::shared_ptr<NES::Experimental::HashMapFactory> hashMapFactory);

    /**
     * @brief Start of the slice.
     * @return uint64_t
     */
    inline uint64_t getStart() const { return start; }

    /**
     * @brief End of the slice.
     * @return uint64_t
     */
    inline uint64_t getEnd() const { return end; }

    /**
     * @brief Checks if a slice covers a specific ts.
     * A slice cover a cover a range from [startTs, endTs - 1]
     * @param ts
     * @return
     */
    inline bool coversTs(uint64_t ts) const { return start <= ts && end > ts; }

    /**
     * @brief State of the slice.
     * @return uint64_t
     */
    inline NES::Experimental::Hashmap& getState() { return state; }

    /**
     * @brief Reinitialize slice.
     * @param start
     * @param end
     */
    void reset(uint64_t start, uint64_t end);
    friend std::ostream& operator<<(std::ostream& os, const KeyedSlice& slice);

  private:
    uint64_t start;
    uint64_t end;
    NES::Experimental::Hashmap state;
};

}// namespace NES::Windowing::Experimental

#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDSLICE_HPP_

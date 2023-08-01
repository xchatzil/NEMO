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

#ifndef NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_MEMORYSOURCETYPE_HPP_
#define NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_MEMORYSOURCETYPE_HPP_

#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Util/GatheringMode.hpp>

namespace NES {

class MemorySourceType;
using MemorySourceTypePtr = std::shared_ptr<MemorySourceType>;

/**
 * @brief A source config for a memory source
 */
class MemorySourceType : public PhysicalSourceType {
  public:
    /**
     * @brief Factory method of MemorySourceType
     * @param memoryArea : location from where to read data
     * @param memoryAreaSize : amount of memory to read
     * @param numBuffersToProduce : number of buffers to produce
     * @param gatheringValue : gathering value
     * @param gatheringMode : gathering mode
     * @param sourceAffinity: sourceAffinity
     * @param taskQueueId: taskQueueId
     * @return a constructed MemorySourceType
     */
    static MemorySourceTypePtr create(uint8_t* memoryArea,
                                      size_t memoryAreaSize,
                                      uint64_t numBuffersToProduce,
                                      uint64_t gatheringValue,
                                      const std::string& gatheringMode,
                                      uint64_t sourceAffinity = 0,
                                      uint64_t taskQueueId = 0);

    const std::shared_ptr<uint8_t>& getMemoryArea() const;

    size_t getMemoryAreaSize() const;

    uint64_t getNumberOfBufferToProduce() const;

    uint64_t getGatheringValue() const;

    GatheringMode::Value getGatheringMode() const;

    uint64_t getTaskQueueId() const;

    uint64_t getSourceAffinity() const;

    /**
     * @brief The string representation of the object
     * @return the string representation of the object
     */
    std::string toString() override;

    bool equal(const PhysicalSourceTypePtr& other) override;

    void reset() override;

  private:
    /**
     * @brief Create a MemorySourceType using a set of parameters
     * @param memoryArea the pointer to the memory area
     * @param memoryAreaSize the size of the memory area
     * @param numBuffersToProduce: number of buffers to produce
     * @param gatheringValue: gather value
     * @param gatheringMode: gathering mode
     * @param sourceAffinity: sourceAffinity
     * @param taskQueueId: taskQueueId
     */
    explicit MemorySourceType(uint8_t* memoryArea,
                              size_t memoryAreaSize,
                              uint64_t numBuffersToProduce,
                              uint64_t gatheringValue,
                              GatheringMode::Value gatheringMode,
                              uint64_t sourceAffinity,
                              uint64_t taskQueueId);

    std::shared_ptr<uint8_t> memoryArea;
    size_t memoryAreaSize;
    uint64_t numberOfBufferToProduce;
    uint64_t gatheringValue;
    GatheringMode::Value gatheringMode;
    uint64_t sourceAffinity;
    uint64_t taskQueueId;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_CATALOGS_SOURCE_PHYSICALSOURCETYPES_MEMORYSOURCETYPE_HPP_

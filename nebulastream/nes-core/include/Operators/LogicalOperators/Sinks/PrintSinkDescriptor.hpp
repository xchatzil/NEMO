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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sinks/SinkDescriptor.hpp>
#include <Util/FaultToleranceType.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical print sink
 */
class PrintSinkDescriptor : public SinkDescriptor {

  public:
    /**
     * @brief Factory method to create a new prink sink descriptor
     * @param faultToleranceType: fault tolerance type of a query
     * @param numberOfOrigins: number of origins of a given query
     * @return descriptor for print sink
     */
    static SinkDescriptorPtr create(FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                                    uint64_t numberOfOrigins = 1);
    std::string toString() override;
    [[nodiscard]] bool equal(SinkDescriptorPtr const& other) override;

    /**
     * @brief getter for fault-tolerance type
     * @return fault-tolerance type
     */
    FaultToleranceType::Value getFaultToleranceType() const;

    /**
     * @brief getter for number of origins
     * @return number of origins
     */
    uint64_t getNumberOfOrigins() const;

  private:
    explicit PrintSinkDescriptor(FaultToleranceType::Value faultToleranceType, uint64_t numberOfOrigins);
};

using PrintSinkDescriptorPtr = std::shared_ptr<PrintSinkDescriptor>;

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_PRINTSINKDESCRIPTOR_HPP_

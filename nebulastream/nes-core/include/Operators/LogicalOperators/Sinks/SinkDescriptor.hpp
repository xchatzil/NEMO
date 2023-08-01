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

#ifndef NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKDESCRIPTOR_HPP_
#define NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKDESCRIPTOR_HPP_

#include <Util/FaultToleranceType.hpp>
#include <memory>

namespace NES {

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

/**
 * @brief This class is used for representing the description of a sink operator
 */
class SinkDescriptor : public std::enable_shared_from_this<SinkDescriptor> {

  public:
    explicit SinkDescriptor(FaultToleranceType::Value faultToleranceType = FaultToleranceType::NONE,
                            uint64_t numberOfOrigins = 1);

    virtual ~SinkDescriptor() = default;

    /**
    * @brief Checks if the current node is of type SinkMedium
    * @tparam SinkType
    * @return bool true if node is of SinkMedium
    */
    template<class SinkType>
    bool instanceOf() {
        if (dynamic_cast<SinkType*>(this)) {
            return true;
        };
        return false;
    };

    /**
     * @brief getter for fault-tolerance type
     * @return fault-tolerance type
     */
    FaultToleranceType::Value getFaultToleranceType() const;

    /**
      * @brief setter for fault-tolerance type
      * @param fault-tolerance type
      */
    void setFaultToleranceType(FaultToleranceType::Value faultToleranceType);

    /**
     * @brief getter for number of origins
     * @return number of origins
     */
    uint64_t getNumberOfOrigins() const;

    /**
    * @brief Dynamically casts the node to a NodeType
    * @tparam NodeType
    * @return returns a shared pointer of the NodeType
    */
    template<class SinkType>
    std::shared_ptr<SinkType> as() {
        if (instanceOf<SinkType>()) {
            return std::dynamic_pointer_cast<SinkType>(this->shared_from_this());
        }
        throw std::bad_cast();
    }

    virtual std::string toString() = 0;
    [[nodiscard]] virtual bool equal(SinkDescriptorPtr const& other) = 0;

  protected:
    FaultToleranceType::Value faultToleranceType;
    uint64_t numberOfOrigins;
};

}// namespace NES

#endif// NES_CORE_INCLUDE_OPERATORS_LOGICALOPERATORS_SINKS_SINKDESCRIPTOR_HPP_

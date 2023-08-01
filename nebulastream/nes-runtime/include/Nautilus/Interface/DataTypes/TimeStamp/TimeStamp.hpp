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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_TIMESTAMP_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_TIMESTAMP_HPP_
#include <Nautilus/Interface/DataTypes/Any.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Nautilus {
/**
 * @brief TimeStamp data type. Customized data type, currently simply holds the timestamp as milliseconds.
 */
class TimeStamp : public Any {
  public:
    static const inline auto type = TypeIdentifier::create<TimeStamp>();

    TimeStamp(Value<> milliseconds);
    Nautilus::IR::Types::StampPtr getType() const override;
    AnyPtr copy() override;
    std::string toString() override;

    /**
    * @brief Addition of two TimeStamps, i.e., their milliseconds
    * @param other the TimeStamp to add to this TimeStamp instance
    */
    AnyPtr add(const TimeStamp& other) const;

    /**
    * @brief Tests if this TimeStamp Value is equal to the other TimeSTamp Value
    * @param other the other TimeStamp
    */
    std::shared_ptr<Boolean> equals(const TimeStamp& other) const;

    /**
    * @brief Tests if this TimeStamp Value is less than the other TimeSTamp Value
    * @param other the other TimeStamp
    */
    std::shared_ptr<Boolean> lessThan(const TimeStamp& other) const;

    /**
    * @brief Tests if this TimeStamp Value is greater than the other TimeSTamp Value
    * @param other the other TimeStamp
    */
    std::shared_ptr<Boolean> greaterThan(const TimeStamp& other) const;

    /**
    * @brief Returns the Milliseconds of the TimeStamp
    */
    Value<> getMilliSeconds();

    /**
    * @brief Returns the Seconds of the TimeStamp
    */
    Value<> getSeconds();

    /**
    * @brief Returns the Minutes of the TimeStamp
    */
    Value<> getMinutes();

    /**
    * @brief Returns the Hours of the TimeStamp
    */
    Value<> getHours();

    /**
    * @brief Returns the Day of the TimeStamp
    */
    Value<> getDays();

    /**
    * @brief Returns the Month of the TimeStamp
    */
    Value<> getMonths();

    /**
    * @brief Returns the Year of the TimeStamp
    */
    Value<> getYears();

    /**
    * @brief Returns the TimeStamp in milliseconds
    * @param other the other TimeStamp
    */
    Value<> getValue();

  private:
    Value<> milliseconds;
};

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_DATATYPES_TIMESTAMP_TIMESTAMP_HPP_

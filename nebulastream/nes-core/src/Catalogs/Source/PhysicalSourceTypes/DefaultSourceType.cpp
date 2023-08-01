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

#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

DefaultSourceTypePtr DefaultSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<DefaultSourceType>(DefaultSourceType(std::move(sourceConfigMap)));
}

DefaultSourceTypePtr DefaultSourceType::create() { return std::make_shared<DefaultSourceType>(DefaultSourceType()); }

DefaultSourceTypePtr DefaultSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<DefaultSourceType>(DefaultSourceType(std::move(yamlConfig)));
}

DefaultSourceType::DefaultSourceType()
    : PhysicalSourceType(DEFAULT_SOURCE), numberOfBuffersToProduce(Configurations::ConfigurationOption<uint32_t>::create(
                                              Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                              1,
                                              "Number of buffers to produce.")),
      sourceGatheringInterval(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG,
                                                                1,
                                                                "Gathering interval of the source.")),
      gatheringMode(
          Configurations::ConfigurationOption<GatheringMode::Value>::create(Configurations::SOURCE_GATHERING_MODE_CONFIG,
                                                                            GatheringMode::INTERVAL_MODE,
                                                                            "Gathering mode of the source.")) {
    NES_INFO("NesSourceConfig: Init source config object with default values.");
}

DefaultSourceType::DefaultSourceType(std::map<std::string, std::string>) : DefaultSourceType() {}

DefaultSourceType::DefaultSourceType(Yaml::Node) : DefaultSourceType() {}

std::string DefaultSourceType::toString() {
    std::stringstream ss;
    ss << "Default Source Type =>{\n";
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG << ":" << numberOfBuffersToProduce->getValue();
    ss << Configurations::SOURCE_GATHERING_INTERVAL_CONFIG << ":" << sourceGatheringInterval->getValue();
    ss << Configurations::SOURCE_GATHERING_MODE_CONFIG << ":" << GatheringMode::toString(gatheringMode->getValue());
    ss << "\n}";
    return ss.str();
}

bool DefaultSourceType::equal(const PhysicalSourceTypePtr& other) { return !other->instanceOf<DefaultSourceType>(); }

const Configurations::IntConfigOption& DefaultSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

const Configurations::IntConfigOption& DefaultSourceType::getSourceGatheringInterval() const { return sourceGatheringInterval; }

Configurations::GatheringModeConfigOption DefaultSourceType::getGatheringMode() const { return gatheringMode; }

void DefaultSourceType::reset() {
    //nothing
}

void DefaultSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduce) {
    this->numberOfBuffersToProduce->setValue(numberOfBuffersToProduce);
}

void DefaultSourceType::setSourceGatheringInterval(uint32_t sourceGatheringInterval) {
    this->sourceGatheringInterval->setValue(sourceGatheringInterval);
}

void DefaultSourceType::setGatheringMode(std::string inputGatheringMode) {
    gatheringMode->setValue(GatheringMode::getFromString(std::move(inputGatheringMode)));
}

void DefaultSourceType::setGatheringMode(GatheringMode::Value inputGatheringMode) { gatheringMode->setValue(inputGatheringMode); }

}// namespace NES
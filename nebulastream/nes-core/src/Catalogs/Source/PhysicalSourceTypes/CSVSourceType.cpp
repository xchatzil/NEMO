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

#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

CSVSourceTypePtr CSVSourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<CSVSourceType>(CSVSourceType(std::move(yamlConfig)));
}

CSVSourceTypePtr CSVSourceType::create() { return std::make_shared<CSVSourceType>(CSVSourceType()); }

CSVSourceTypePtr CSVSourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<CSVSourceType>(CSVSourceType(std::move(sourceConfigMap)));
}

CSVSourceType::CSVSourceType()
    : PhysicalSourceType(CSV_SOURCE),
      filePath(Configurations::ConfigurationOption<std::string>::create(Configurations::FILE_PATH_CONFIG,
                                                                        "",
                                                                        "file path, needed for: CSVSource, BinarySource")),
      skipHeader(Configurations::ConfigurationOption<bool>::create(Configurations::SKIP_HEADER_CONFIG,
                                                                   false,
                                                                   "Skip first line of the file.")),
      delimiter(
          Configurations::ConfigurationOption<std::string>::create(Configurations::DELIMITER_CONFIG,
                                                                   ",",
                                                                   "delimiter for distinguishing between values in a file")),
      numberOfBuffersToProduce(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG,
                                                                1,
                                                                "Number of buffers to produce.")),
      numberOfTuplesToProducePerBuffer(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG,
                                                                1,
                                                                "Number of tuples to produce per buffer.")),
      sourceGatheringInterval(
          Configurations::ConfigurationOption<uint32_t>::create(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG,
                                                                1,
                                                                "Gathering interval of the source.")),
      gatheringMode(
          Configurations::ConfigurationOption<GatheringMode::Value>::create(Configurations::SOURCE_GATHERING_MODE_CONFIG,
                                                                            GatheringMode::INTERVAL_MODE,
                                                                            "Gathering mode of the source.")) {
    NES_INFO("CSVSourceTypeConfig: Init source config object with default values.");
}

CSVSourceType::CSVSourceType(std::map<std::string, std::string> sourceConfigMap) : CSVSourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from command line.");
    if (sourceConfigMap.find(Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find(Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (sourceConfigMap.find(Configurations::DELIMITER_CONFIG) != sourceConfigMap.end()) {
        delimiter->setValue(sourceConfigMap.find(Configurations::DELIMITER_CONFIG)->second);
    }
    if (sourceConfigMap.find(Configurations::SKIP_HEADER_CONFIG) != sourceConfigMap.end()) {
        skipHeader->setValue((sourceConfigMap.find(Configurations::SKIP_HEADER_CONFIG)->second == "true"));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG) != sourceConfigMap.end()) {
        numberOfBuffersToProduce->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG) != sourceConfigMap.end()) {
        numberOfTuplesToProducePerBuffer->setValue(
            std::stoi(sourceConfigMap.find(Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG) != sourceConfigMap.end()) {
        sourceGatheringInterval->setValue(
            std::stoi(sourceConfigMap.find(Configurations::SOURCE_GATHERING_INTERVAL_CONFIG)->second));
    }
    if (sourceConfigMap.find(Configurations::SOURCE_GATHERING_MODE_CONFIG) != sourceConfigMap.end()) {
        gatheringMode->setValue(
            GatheringMode::getFromString(sourceConfigMap.find(Configurations::SOURCE_GATHERING_MODE_CONFIG)->second));
    }
}

CSVSourceType::CSVSourceType(Yaml::Node yamlConfig) : CSVSourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from YAML file.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("CSVSourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
    if (!yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>() != "\n") {
        delimiter->setValue(yamlConfig[Configurations::DELIMITER_CONFIG].As<std::string>());
    }
    if (!yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<std::string>() != "\n") {
        skipHeader->setValue(yamlConfig[Configurations::SKIP_HEADER_CONFIG].As<bool>());
    }
    if (!yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<std::string>() != "\n") {
        numberOfBuffersToProduce->setValue(yamlConfig[Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<std::string>() != "\n") {
        numberOfTuplesToProducePerBuffer->setValue(
            yamlConfig[Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<std::string>() != "\n") {
        sourceGatheringInterval->setValue(yamlConfig[Configurations::SOURCE_GATHERING_INTERVAL_CONFIG].As<uint32_t>());
    }
    if (!yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>() != "\n") {
        gatheringMode->setValue(
            GatheringMode::getFromString(yamlConfig[Configurations::SOURCE_GATHERING_MODE_CONFIG].As<std::string>()));
    }
}

std::string CSVSourceType::toString() {
    std::stringstream ss;
    ss << "CSVSource Type => {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << Configurations::SKIP_HEADER_CONFIG + ":" + skipHeader->toStringNameCurrentValue();
    ss << Configurations::DELIMITER_CONFIG + ":" + delimiter->toStringNameCurrentValue();
    ss << Configurations::SOURCE_GATHERING_INTERVAL_CONFIG + ":" + sourceGatheringInterval->toStringNameCurrentValue();
    ss << Configurations::SOURCE_GATHERING_MODE_CONFIG + ":" + GatheringMode::toString(gatheringMode->getValue()) << "\n";
    ss << Configurations::NUMBER_OF_BUFFERS_TO_PRODUCE_CONFIG + ":" + numberOfBuffersToProduce->toStringNameCurrentValue();
    ss << Configurations::NUMBER_OF_TUPLES_TO_PRODUCE_PER_BUFFER_CONFIG + ":"
            + numberOfTuplesToProducePerBuffer->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool CSVSourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<CSVSourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<CSVSourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue()
        && skipHeader->getValue() == otherSourceConfig->skipHeader->getValue()
        && delimiter->getValue() == otherSourceConfig->delimiter->getValue()
        && sourceGatheringInterval->getValue() == otherSourceConfig->sourceGatheringInterval->getValue()
        && gatheringMode->getValue() == otherSourceConfig->gatheringMode->getValue()
        && numberOfBuffersToProduce->getValue() == otherSourceConfig->numberOfBuffersToProduce->getValue()
        && numberOfTuplesToProducePerBuffer->getValue() == otherSourceConfig->numberOfTuplesToProducePerBuffer->getValue();
}

Configurations::StringConfigOption CSVSourceType::getFilePath() const { return filePath; }

Configurations::BoolConfigOption CSVSourceType::getSkipHeader() const { return skipHeader; }

Configurations::StringConfigOption CSVSourceType::getDelimiter() const { return delimiter; }

Configurations::IntConfigOption CSVSourceType::getGatheringInterval() const { return sourceGatheringInterval; }

Configurations::IntConfigOption CSVSourceType::getNumberOfBuffersToProduce() const { return numberOfBuffersToProduce; }

Configurations::IntConfigOption CSVSourceType::getNumberOfTuplesToProducePerBuffer() const {
    return numberOfTuplesToProducePerBuffer;
}

Configurations::GatheringModeConfigOption CSVSourceType::getGatheringMode() const { return gatheringMode; }

void CSVSourceType::setSkipHeader(bool skipHeaderValue) { skipHeader->setValue(skipHeaderValue); }

void CSVSourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void CSVSourceType::setDelimiter(std::string delimiterValue) { delimiter->setValue(std::move(delimiterValue)); }

void CSVSourceType::setGatheringInterval(uint32_t sourceGatheringIntervalValue) {
    sourceGatheringInterval->setValue(sourceGatheringIntervalValue);
}

void CSVSourceType::setNumberOfBuffersToProduce(uint32_t numberOfBuffersToProduceValue) {
    numberOfBuffersToProduce->setValue(numberOfBuffersToProduceValue);
}

void CSVSourceType::setNumberOfTuplesToProducePerBuffer(uint32_t numberOfTuplesToProducePerBufferValue) {
    numberOfTuplesToProducePerBuffer->setValue(numberOfTuplesToProducePerBufferValue);
}

void CSVSourceType::setGatheringMode(std::string inputGatheringMode) {
    gatheringMode->setValue(GatheringMode::getFromString(std::move(inputGatheringMode)));
}

void CSVSourceType::setGatheringMode(GatheringMode::Value inputGatheringMode) { gatheringMode->setValue(inputGatheringMode); }

void CSVSourceType::reset() {
    setFilePath(filePath->getDefaultValue());
    setSkipHeader(skipHeader->getDefaultValue());
    setDelimiter(delimiter->getDefaultValue());
    setNumberOfBuffersToProduce(numberOfBuffersToProduce->getDefaultValue());
    setNumberOfTuplesToProducePerBuffer(numberOfTuplesToProducePerBuffer->getDefaultValue());
    setGatheringInterval(sourceGatheringInterval->getDefaultValue());
    setGatheringMode(gatheringMode->getDefaultValue());
}

}// namespace NES

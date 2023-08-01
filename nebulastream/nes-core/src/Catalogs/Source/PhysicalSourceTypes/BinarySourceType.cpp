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

#include <Catalogs/Source/PhysicalSourceTypes/BinarySourceType.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>
#include <utility>

namespace NES {

BinarySourceTypePtr BinarySourceType::create(Yaml::Node yamlConfig) {
    return std::make_shared<BinarySourceType>(BinarySourceType(yamlConfig));
}

BinarySourceType::BinarySourceType(Yaml::Node yamlConfig) : BinarySourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from YAML.");
    if (!yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>().empty()
        && yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>() != "\n") {
        filePath->setValue(yamlConfig[Configurations::FILE_PATH_CONFIG].As<std::string>());
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
}

BinarySourceTypePtr BinarySourceType::create(std::map<std::string, std::string> sourceConfigMap) {
    return std::make_shared<BinarySourceType>(BinarySourceType(std::move(sourceConfigMap)));
}

BinarySourceType::BinarySourceType(std::map<std::string, std::string> sourceConfigMap) : BinarySourceType() {
    NES_INFO("CSVSourceType: Init default CSV source config object with values from command line.");
    if (sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG) != sourceConfigMap.end()) {
        filePath->setValue(sourceConfigMap.find("--" + Configurations::FILE_PATH_CONFIG)->second);
    } else {
        NES_THROW_RUNTIME_ERROR("BinarySourceType:: no filePath defined! Please define a filePath using "
                                << Configurations::FILE_PATH_CONFIG << " configuration.");
    }
}

BinarySourceTypePtr BinarySourceType::create() { return std::make_shared<BinarySourceType>(BinarySourceType()); }

BinarySourceType::BinarySourceType()
    : PhysicalSourceType(BINARY_SOURCE), filePath(Configurations::ConfigurationOption<std::string>::create(
                                             Configurations::FILE_PATH_CONFIG,
                                             "../tests/test_data/QnV_short.csv",//FIXME: What should we do about these things?
                                             "file path, needed for: CSVSource, BinarySource")) {
    NES_INFO("BinarySourceTypeConfig: Init source config object with default params.");
}

std::string BinarySourceType::toString() {
    std::stringstream ss;
    ss << "BinarySource = {\n";
    ss << Configurations::FILE_PATH_CONFIG + ":" + filePath->toStringNameCurrentValue();
    ss << "\n}";
    return ss.str();
}

bool BinarySourceType::equal(const PhysicalSourceTypePtr& other) {
    if (!other->instanceOf<BinarySourceType>()) {
        return false;
    }
    auto otherSourceConfig = other->as<BinarySourceType>();
    return filePath->getValue() == otherSourceConfig->filePath->getValue();
}

Configurations::StringConfigOption BinarySourceType::getFilePath() const { return filePath; }

void BinarySourceType::setFilePath(std::string filePathValue) { filePath->setValue(std::move(filePathValue)); }

void BinarySourceType::reset() { setFilePath(filePath->getDefaultValue()); }

}// namespace NES
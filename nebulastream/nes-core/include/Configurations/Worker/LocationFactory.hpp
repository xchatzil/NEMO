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
#ifndef NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_LOCATIONFACTORY_HPP_
#define NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_LOCATIONFACTORY_HPP_
#include <Util/yaml/Yaml.hpp>
#include <map>
#include <memory>
#include <string>

namespace NES::Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
}// namespace NES::Spatial::Index::Experimental

namespace NES::Configurations::Spatial::Index::Experimental {

class LocationFactory {

  public:
    /**
     * @brief obtains a Geographical location objects by parsing string coordinates
     * @param str: Coordinate string in the format "<lat, lng>"
     * @return A geographical location with the coordinates from the string, or <200, 200> (representing invalid Coordinates)
     * if the string was empty
     */

    static ::NES::Spatial::Index::Experimental::Location createFromString(std::string,
                                                                          std::map<std::string, std::string>& commandLineParams);

    /**
     * @brief obtains a Geographical location objects from yaml config
     * @param yamlConfig: a yaml config obtained from a file containing "fixedLocationCoordinates: <lat, lng>"
     * @return A geographical location with the coordinates from the config entry, or <200, 200> (representing invalid coordinates)
     * if the string was empty
     */
    static ::NES::Spatial::Index::Experimental::Location createFromYaml(Yaml::Node& yamlConfig);
};
}// namespace NES::Configurations::Spatial::Index::Experimental
#endif// NES_CORE_INCLUDE_CONFIGURATIONS_WORKER_LOCATIONFACTORY_HPP_

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
#ifndef NES_CORE_INCLUDE_REST_CONTROLLER_UDFCATALOGCONTROLLER_HPP_
#define NES_CORE_INCLUDE_REST_CONTROLLER_UDFCATALOGCONTROLLER_HPP_
#include <API/Schema.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <Catalogs/UDF/PythonUdfDescriptor.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Catalogs/UDF/UdfDescriptor.hpp>
#include <Exceptions/UdfException.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <GRPC/Serialization/UdfSerializationUtil.hpp>
#include <REST/Controller/BaseRouterPrefix.hpp>
#include <REST/Handlers/ErrorHandler.hpp>
#include <UdfCatalogService.pb.h>
#include <nlohmann/json.hpp>
#include <oatpp/core/macro/codegen.hpp>
#include <oatpp/core/macro/component.hpp>
#include <oatpp/web/server/api/ApiController.hpp>
#include OATPP_CODEGEN_BEGIN(ApiController)

namespace NES {

namespace Catalogs::UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace Catalogs::UDF

namespace REST {

namespace Controller {

using namespace Catalogs::UDF;

class UdfCatalogController : public oatpp::web::server::api::ApiController {

  public:
    /**
     * Constructor with object mapper.
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param udfCatalog - catalog for user defined functions
     * @param completeRouterPrefix - url consisting of base router prefix (e.g "v1/nes/") and controller specific router prefix (e.g "connectivityController")
     * @param errorHandler - responsible for handling errors
     */
    UdfCatalogController(const std::shared_ptr<ObjectMapper>& objectMapper,
                         UdfCatalogPtr udfCatalog,
                         oatpp::String completeRouterPrefix,
                         ErrorHandlerPtr errorHandler)
        : oatpp::web::server::api::ApiController(objectMapper, completeRouterPrefix), udfCatalog(udfCatalog),
          errorHandler(errorHandler) {}

    /**
     * Create a shared object of the API controller
     * @param objectMapper - default object mapper used to serialize/deserialize DTOs.
     * @param udfCatalog - catalog for user defined functions
     * @param routerPrefixAddition - controller specific router prefix (e.g "connectivityController/")
     * @param errorHandler - responsible for handling errors
     */
    static std::shared_ptr<UdfCatalogController> create(const std::shared_ptr<ObjectMapper>& objectMapper,
                                                        UdfCatalogPtr udfCatalog,
                                                        std::string routerPrefixAddition,
                                                        ErrorHandlerPtr errorHandler) {
        oatpp::String completeRouterPrefix = BASE_ROUTER_PREFIX + routerPrefixAddition;
        return std::make_shared<UdfCatalogController>(objectMapper, udfCatalog, completeRouterPrefix, errorHandler);
    }

    /**
     *  Endpoint to retrieve udf descriptor
     *  returns 200 and descriptor if present
     *  returns 400 if request doesnt contain udf as query parameter
     *  returns 404 if no udf found with given name
     *  returns 500 for internal server errors
     * @param udf : name of udf to retrieve
     */
    ENDPOINT("GET", "/getUdfDescriptor", getUdfDescriptor, QUERY(String, udf, "udfName")) {
        try {
            std::string udfName = udf.getValue("");
            auto udfDescriptor = UdfDescriptor::as<JavaUdfDescriptor>(udfCatalog->getUdfDescriptor(udfName));
            GetJavaUdfDescriptorResponse response;
            if (udfDescriptor == nullptr) {
                // Signal that the UDF does not exist in the catalog.
                NES_DEBUG("REST client tried retrieving UDF descriptor for non-existing Java UDF: " << udfName);
                response.set_found(false);
                return createResponse(Status::CODE_404, response.SerializeAsString());
            } else {
                // Return the UDF descriptor to the client.
                NES_DEBUG("Returning UDF descriptor to REST client for Java UDF: " << udfName);
                response.set_found(true);
                UdfSerializationUtil::serializeJavaUdfDescriptor(*udfDescriptor, *response.mutable_java_udf_descriptor());
                return createResponse(Status::CODE_200, response.SerializeAsString());
            }
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server error");
        }
    }

    /**
     * Endpoint to retrieve names of all udfs
     * returns 200 and list of udf names. Lists can be empty
     * returns 500 for internal server errors
     *
     */
    ENDPOINT("GET", "/listUdfs", listUdfs) {
        try {
            nlohmann::json response;
            response["udfs"] = nlohmann::json::array();
            for (const auto& udf : udfCatalog->listUdfs()) {
                response["udfs"].push_back(udf);
            }
            return createResponse(Status::CODE_200, response.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server error");
        }
    }

    /**
     * Endpoint to register a java udf
     * Request body must contain a protobuf message serialized as string
     * returns 200 if java udf was successfully registered
     * returns 400 if request body is emtpy or if errors occur parsing protobuf message into a JavaUdfDescriptor object
     * returns 500 for internal server errors
     */
    ENDPOINT("POST", "/registerJavaUdf", registerJavaUdf, BODY_STRING(String, request)) {
        try {
            // Convert protobuf message contents to JavaUdfDescriptor.
            std::string body = request.getValue("");
            if (body.empty()) {
                errorHandler->handleError(Status::CODE_400, "Protobuf message is empty");
            }
            NES_DEBUG("Parsing Java UDF descriptor from REST request");
            auto javaUdfRequest = RegisterJavaUdfRequest{};
            javaUdfRequest.ParseFromString(body);
            auto descriptorMessage = javaUdfRequest.java_udf_descriptor();
            auto javaUdfDescriptor = UdfSerializationUtil::deserializeJavaUdfDescriptor(descriptorMessage);
            // Register JavaUdfDescriptor in UDF catalog and return success.
            NES_DEBUG("Registering Java UDF '" << javaUdfRequest.udf_name() << "'.'");
            udfCatalog->registerUdf(javaUdfRequest.udf_name(), javaUdfDescriptor);
            return createResponse(Status::CODE_200, "Registered Java UDF");
        } catch (const UdfException& e) {
            NES_WARNING("Exception occurred during UDF registration: " << e.what());
            // Just return the exception message to the client, not the stack trace.
            return errorHandler->handleError(Status::CODE_400, e.getMessage());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server error");
        }
    }

    /**
     * Endpoint for deleting udfs
     * returns 200 if delete is successful or no such udf found
     * returns 500 for internal server errors
     * @param udf : name of udf to delete
     */
    ENDPOINT("DELETE", "/removeUdf", removeUdf, QUERY(String, udf, "udfName")) {
        try {
            std::string udfName = udf.getValue("");
            NES_DEBUG("Removing Java UDF '" << udfName << "'");
            auto removed = udfCatalog->removeUdf(udfName);
            nlohmann::json result;
            result["removed"] = removed;
            return createResponse(Status::CODE_200, result.dump());
        } catch (...) {
            return errorHandler->handleError(Status::CODE_500, "Internal Server error");
        }
    }

  private:
    UdfCatalogPtr udfCatalog;
    ErrorHandlerPtr errorHandler;
};
}// namespace Controller
}// namespace REST
}// namespace NES
#include OATPP_CODEGEN_END(ApiController)
#endif// NES_CORE_INCLUDE_REST_CONTROLLER_UDFCATALOGCONTROLLER_HPP_

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

#include <API/Query.hpp>
#include <Catalogs/UDF/JavaUdfDescriptor.hpp>
#include <NesBaseTest.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/JavaUdfDescriptorBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ProtobufMessageFactory.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <memory>
#include <nes-grpc/UdfCatalogService.pb.h>
#include <nlohmann/json.hpp>
#include <oatpp/web/protocol/http/Http.hpp>

namespace NES {
using namespace std::string_literals;
using namespace NES::Catalogs;
using namespace google::protobuf;
using namespace oatpp::web::protocol::http;

class UdfCatalogControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }

    static void verifyResponseResult(const cpr::Response& response, const nlohmann::json expected) {
        NES_DEBUG(response.text);
        auto responseJson = nlohmann::json::parse(response.text);
        ASSERT_TRUE(responseJson == expected);
    }

    static void verifySerializedInstance(const UDF::JavaSerializedInstance& actual, const std::string& expected) {
        auto converted = UDF::JavaSerializedInstance{expected.begin(), expected.end()};
        ASSERT_EQ(actual, converted);
    }

    static void
    verifyByteCodeList(const UDF::JavaUdfByteCodeList& actual,
                       const google::protobuf::RepeatedPtrField<JavaUdfDescriptorMessage_JavaUdfClassDefinition>& expected) {
        ASSERT_EQ(actual.size(), static_cast<decltype(actual.size())>(expected.size()));
        for (const auto& classDefinition : expected) {
            auto actualByteCode = actual.find(classDefinition.class_name());
            ASSERT_TRUE(actualByteCode != actual.end());
            auto converted = UDF::JavaByteCode(classDefinition.byte_code().begin(), classDefinition.byte_code().end());
            ASSERT_EQ(actualByteCode->second, converted);
        }
    }

    [[nodiscard]] static GetJavaUdfDescriptorResponse extractGetJavaUdfDescriptorResponse(const cpr::Response& response) {
        GetJavaUdfDescriptorResponse udfResponse;
        udfResponse.ParseFromString(response.text);
        return udfResponse;
    }

    void startCoordinator() {
        NES_INFO("UdfCatalogController: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("UdfCatalogControllerTest: Coordinator started successfully");
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

//Test if retrieval of a UDF added directly to UdfCatalog over POST returns same UDF
TEST_F(UdfCatalogControllerTest, getUdfDescriptorReturnsUdf) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUdfCatalog();

    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();

    //register udf with coordinator
    udfCatalog->registerUdf(udfName, javaUdfDescriptor);

    //send a GET request to REST API of coordinator for the previously defined java udf
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"},
                                cpr::Parameters{{"udfName", udfName}});
    future.wait();
    auto response = future.get();
    //check if response code indicates a udf has been retrieved
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    // extract protobuf message from string response
    GetJavaUdfDescriptorResponse udfResponse = extractGetJavaUdfDescriptorResponse(response);
    // from protobuf message, get the java udf descriptor
    auto descriptor = udfResponse.java_udf_descriptor();
    // and compare udf descriptor with the one registered to the coordinator earlier
    ASSERT_EQ(javaUdfDescriptor->getClassName(), descriptor.udf_class_name());
    ASSERT_EQ(javaUdfDescriptor->getMethodName(), descriptor.udf_method_name());
    verifySerializedInstance(javaUdfDescriptor->getSerializedInstance(), descriptor.serialized_instance());
    verifyByteCodeList(javaUdfDescriptor->getByteCodeList(), descriptor.classes());
}

// Test behavior of GET for a UDF that doesn't exist
TEST_F(UdfCatalogControllerTest, testGetUdfDescriptorIfNoUdfExists) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUdfCatalog();

    std::string udfName = "my_udf";
    //send a GET request to REST API of coordinator for an udf that doesn't exist
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"},
                                cpr::Parameters{{"udfName", udfName}});

    future.wait();
    auto response = future.get();
    //check that status code indicates specified udf doesn't exist
    ASSERT_EQ(response.status_code, Status::CODE_404.code);
    // and compare contents of response
    // extract protobuf message from string response
    GetJavaUdfDescriptorResponse udfResponse = extractGetJavaUdfDescriptorResponse(response);
    ASSERT_TRUE(!udfResponse.found() && !udfResponse.has_java_udf_descriptor());
}

//Test if Oatpp framework correctly returns 404 when endpoint isn't defined
TEST_F(UdfCatalogControllerTest, testErrorIfUnknownEndpointIsUsed) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    //create a request to an endpoint that isn't defined
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/SUPER_SECRET_URL"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{"Whats the object-oriented way to become wealthy? Inheritance."});

    future.wait();
    auto response = future.get();
    // and see if the response code is 404
    ASSERT_EQ(response.status_code, Status::CODE_404.code);
}

//Test if RegisterJavaUdf endpoint handles exceptions without returning a stack trace
TEST_F(UdfCatalogControllerTest, testIfRegisterEndpointHandlesExceptionsWithoutReturningAStackTrace) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    // given a REST message containing a wrongly formed Java UDF (bytecode list is empty)
    auto javaUdfRequest = ProtobufMessageFactory::createDefaultRegisterJavaUdfRequest();
    javaUdfRequest.mutable_java_udf_descriptor()->clear_classes();

    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/registerJavaUdf"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{javaUdfRequest.SerializeAsString()});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
    // make sure the response does not contain the stack trace
    ASSERT_TRUE(response.text.find("Stack trace") == std::string::npos);
}

//Test if registerJavaUdf endpoint correctly adds java udf
TEST_F(UdfCatalogControllerTest, testIfRegisterUdfEndpointCorrectlyAddsUDF) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    auto udfCatalog = coordinator->getUdfCatalog();
    //check to see if no udfs registered
    ASSERT_TRUE(udfCatalog->listUdfs().empty());
    // create a javaUdfRequest
    auto javaUdfRequest = ProtobufMessageFactory::createDefaultRegisterJavaUdfRequest();

    // submit the javaUdfRequest to the registerJavaUdf endpoint
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/registerJavaUdf"},
                                 cpr::Header{{"Content-Type", "text/plain"}},
                                 cpr::Body{javaUdfRequest.SerializeAsString()});
    future.wait();
    auto response = future.get();
    // then the HTTP response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    ASSERT_EQ(response.text, "Registered Java UDF");
    // check to see if a udf has been added to the udf catalog
    ASSERT_FALSE(udfCatalog->listUdfs().empty());
    // get udf catalog entry
    auto descriptorFromCoordinator = UDF::UdfDescriptor::as<UDF::JavaUdfDescriptor>(udfCatalog->getUdfDescriptor("my_udf"));
    //extract the udf descriptor from the udf post request
    JavaUdfDescriptorMessage descriptorFromPostRequest = javaUdfRequest.java_udf_descriptor();
    //and compare its fields to the java udf catalog entry now found in the coordinator
    ASSERT_EQ(descriptorFromCoordinator->getClassName(), descriptorFromPostRequest.udf_class_name());
    ASSERT_EQ(descriptorFromCoordinator->getMethodName(), descriptorFromPostRequest.udf_method_name());
    verifySerializedInstance(descriptorFromCoordinator->getSerializedInstance(), descriptorFromPostRequest.serialized_instance());
    verifyByteCodeList(descriptorFromCoordinator->getByteCodeList(), descriptorFromPostRequest.classes());
}

//Test if deleting an Udf catalog entry works as expected
TEST_F(UdfCatalogControllerTest, testRemoveUdfEndpoint) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUdfCatalog();

    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();

    //register udf with coordinator
    udfCatalog->registerUdf(udfName, javaUdfDescriptor);

    // given the UDF catalog contains a Java UDF
    // when a REST message is passed to the controller to remove the UDF
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", udfName}});
    future.wait();
    auto response = future.get();
    // then the response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    // and the UDF no longer exists in the catalog
    ASSERT_EQ(UDF::UdfDescriptor::as<UDF::JavaUdfDescriptor>(udfCatalog->getUdfDescriptor("my_udf")), nullptr);
    // and the response shows that the UDF was removed
    nlohmann::json json;
    json["removed"] = true;
    auto responseJson = nlohmann::json::parse(response.text);
    verifyResponseResult(response, json);
}

//Test if removeUdf endpoint handles non-existent UDF correctly
TEST_F(UdfCatalogControllerTest, testRemoveUdfEndpointIfUdfDoesNotExist) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller to remove a UDF that does not exist
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", "my_udf"}});
    future.wait();
    auto response = future.get();
    // then the response is NOT FOUND
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    // and the response shows that the UDF was not removed
    nlohmann::json json;
    json["removed"] = false;
    verifyResponseResult(response, json);
}

//Test if removeUdf endpoint handles missing query parameter correctly
TEST_F(UdfCatalogControllerTest, testRemoveUdfEndpointHandlesMissingQueryParametersCorrectly) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
}

//Test if removeUdf endpoint handles extra query parameters correctly
TEST_F(UdfCatalogControllerTest, testIfRemoveUdfEndpointHandlesExtraQueryParametersCorrectly) {
    // Oatpp framework ignores all query parameters that aren't defined in the endpoint, effectively ignoring them.
    // Superfluous query parameters have no effect on the behavior of an endpoint
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is contains parameters other than the udfName parameter
    auto future = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/removeUdf"},
                                   cpr::Parameters{{"udfName", "my_udf"}, {"meaning_of_life", "42"}});
    future.wait();
    auto response = future.get();
    // then the response is NOT FOUND
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    nlohmann::json json;
    json["removed"] = false;
    verifyResponseResult(response, json);
}

//Test if listUdfs endpoint handles missing query parameters correctly
TEST_F(UdfCatalogControllerTest, testIfListUdfsEndpointHandlesMissingQueryParameters) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // when a REST message is passed to the controller that is missing the udfName parameter
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/getUdfDescriptor"});
    future.wait();
    auto response = future.get();
    // then the response is BadRequest
    ASSERT_EQ(response.status_code, Status::CODE_400.code);
}

//Test if listUdfs endpoint behaves as expected when all else is correct
TEST_F(UdfCatalogControllerTest, testIfListUdfsEndpointReturnsListAsExpected) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUdfCatalog();

    // create a Java Udf descriptor and specify an udf name
    std::string udfName = "my_udf";
    auto javaUdfDescriptor = Catalogs::UDF::JavaUdfDescriptorBuilder::createDefaultJavaUdfDescriptor();
    //register udf with coordinator
    udfCatalog->registerUdf(udfName, javaUdfDescriptor);

    // when a REST message is passed to the controller to get a list of the UDFs
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/listUdfs"});
    future.wait();
    auto response = future.get();
    // then the response is OK
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    // and the response message contains a list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs = udfCatalog->listUdfs();
    json["udfs"] = udfs;
    verifyResponseResult(response, json);
}

//Test if listUdfs endpoint behaves correctly when no UDFs are registered
TEST_F(UdfCatalogControllerTest, testIfListUdfsReturnsEmptyUdfList) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    auto udfCatalog = coordinator->getUdfCatalog();
    // when a REST message is passed to the controller to get a list of the UDFs
    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/udfCatalog/listUdfs"});
    // then the response is OK
    future.wait();
    auto response = future.get();
    ASSERT_EQ(response.status_code, Status::CODE_200.code);
    // and the response message contains an empty list of UDFs
    nlohmann::json json;
    std::vector<std::string> udfs = udfCatalog->listUdfs();
    ;
    json["udfs"] = udfs;
    verifyResponseResult(response, json);
}
}// namespace NES
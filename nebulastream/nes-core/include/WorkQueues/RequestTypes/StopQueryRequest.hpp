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

#ifndef NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_STOPQUERYREQUEST_HPP_
#define NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_STOPQUERYREQUEST_HPP_

#include <WorkQueues/RequestTypes/Request.hpp>

namespace NES {

class StopQueryRequest;
using StopQueryRequestPtr = std::shared_ptr<StopQueryRequest>;

/**
 * @brief This request is used for stopping a running query in NES cluster
 */
class StopQueryRequest : public Request {

  public:
    /**
     * @brief Create instance of  StopQueryRequest
     * @param queryId : the id of query to be stopped
     * @return shared pointer to the instance of stop query request
     */
    static StopQueryRequestPtr create(QueryId queryId);

    std::string toString() override;

    ~StopQueryRequest() override = default;

    QueryId getQueryId() const;

  private:
    explicit StopQueryRequest(QueryId queryId);
    QueryId queryId;
};
}// namespace NES

#endif// NES_CORE_INCLUDE_WORKQUEUES_REQUESTTYPES_STOPQUERYREQUEST_HPP_

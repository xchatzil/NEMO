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

#ifndef NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_
#define NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_

#include <Common/Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class SourceLogicalOperatorNode;
using SourceLogicalOperatorNodePtr = std::shared_ptr<SourceLogicalOperatorNode>;

class SinkLogicalOperatorNode;
using SinkLogicalOperatorNodePtr = std::shared_ptr<SinkLogicalOperatorNode>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

/**
 * @brief This class is responsible for storing all currently running and to be deployed QueryPlans in the NES system.
 * The QueryPlans included in the GlobalQueryPlan can be fused together and therefore each operator in GQP contains
 * information about the set of queryIdAndCatalogEntryMapping it belongs to. The QueryPlans are bound together by a dummy logical root operator.
 */
class GlobalQueryPlan {
  public:
    static GlobalQueryPlanPtr create();

    /**
     * @brief Add query plan to the collection of query plans to be merged
     * @param queryPlan : new query plan to be merged.
     * @return: true if successful else false
     */
    bool addQueryPlan(const QueryPlanPtr& queryPlan);

    /**
     * @brief Create a new shared query plan using the input query plan
     * @param queryPlan : query plan to construct shared query plan.
     * @return: true if successful else false
     */
    bool createNewSharedQueryPlan(const QueryPlanPtr& queryPlan);

    /**
     * @brief remove the operators belonging to the query with input query Id from the global query plan
     * @param queryId: the id of the query whose operators need to be removed
     * @param requestType: request type for query removal
     */
    void removeQuery(QueryId queryId, RequestType::Value requestType);

    /**
     * @brief This method will remove all empty shared query plans that are deployed
     */
    void removeFailedOrStoppedSharedQueryPlans();

    /**
     * @brief Get the all the Query Meta Data to be deployed
     * @return vector of global query meta data to be deployed
     */
    std::vector<SharedQueryPlanPtr> getSharedQueryPlansToDeploy();

    /**
     * @brief Get all shared query plans in the global query plan
     * @return vector of shared query plans
     */
    std::vector<SharedQueryPlanPtr> getAllSharedQueryPlans();

    /**
     * @brief Get the global query id for the query
     * @param queryId: the original query id
     * @return the corresponding global query id
     */
    SharedQueryId getSharedQueryId(QueryId queryId);

    /**
     * @brief Get all query ids associated with the shared query plan id
     * @param sharedQueryPlanId : the id of the shared query plan
     * @return vector of query ids associated to the input shared query plan id
     */
    std::vector<QueryId> getQueryIds(SharedQueryId sharedQueryPlanId);

    /**
     * @brief Get the shared query metadata information for given shared query id
     * @param sharedQueryId : the shared query id
     * @return SharedQueryPlan or nullptr
     */
    SharedQueryPlanPtr getSharedQueryPlan(SharedQueryId sharedQueryId);

    /**
     * @brief Update the global query meta data information
     * @param sharedQueryPlan: the global query metadata to be updated
     * @return true if successful
     */
    bool updateSharedQueryPlan(const SharedQueryPlanPtr& sharedQueryPlan);

    /**
     * Get query plans to add in the Global query plan
     * @return vector of query plans to add
     */
    const std::vector<QueryPlanPtr>& getQueryPlansToAdd() const;

    /**
     * Clear all query plans that need to be added to the global query plan
     * @return true if successfully cleared else false
     */
    bool clearQueryPlansToAdd();

    /**
     * Fetch the Shared query plan consuming the sources with the input source names
     * @param sourceNames: the concatenated names of the logical sources
     * @return pointer to the Shared Query Plan or nullptr
     */
    std::vector<SharedQueryPlanPtr> getSharedQueryPlansConsumingSources(std::string sourceNames);

  private:
    GlobalQueryPlan();

    std::map<std::string, std::vector<SharedQueryPlanPtr>> sourceNamesToSharedQueryPlanMap;
    std::vector<QueryPlanPtr> queryPlansToAdd;
    std::map<QueryId, SharedQueryId> queryIdToSharedQueryIdMap;
    std::map<SharedQueryId, SharedQueryPlanPtr> sharedQueryIdToPlanMap;
};
}// namespace NES
#endif// NES_CORE_INCLUDE_PLANS_GLOBAL_QUERY_GLOBALQUERYPLAN_HPP_

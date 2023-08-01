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

#ifndef NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYCATALOGENTRY_HPP_
#define NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYCATALOGENTRY_HPP_

#include <Common/Identifiers.hpp>
#include <Util/PlacementStrategy.hpp>
#include <Util/QueryStatus.hpp>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace NES {

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class QuerySubPlanMetaData;
using QuerySubPlanMetaDataPtr = std::shared_ptr<QuerySubPlanMetaData>;

namespace Catalogs::Query {

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queryIdAndCatalogEntryMapping map)
 * @param queryString: string representation of the query
 * @param QueryPtr: a pointer to the query
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
  public:
    explicit QueryCatalogEntry(QueryId queryId,
                               std::string queryString,
                               std::string queryPlacementStrategy,
                               QueryPlanPtr inputQueryPlan,
                               QueryStatus::Value queryStatus);

    /**
     * @brief method to get the id of the query
     * @return query id
     */
    [[nodiscard]] QueryId getQueryId() const noexcept;

    /**
     * @brief method to get the string of the query
     * @return query string
     */
    [[nodiscard]] std::string getQueryString() const;

    /**
     * @brief method to get the input query plan
     * @return pointer to the query plan
     */
    [[nodiscard]] QueryPlanPtr getInputQueryPlan() const;

    /**
     * @brief method to get the executed query plan
     * @return pointer to the query plan
     */
    [[nodiscard]] QueryPlanPtr getExecutedQueryPlan() const;

    /**
     * @brief method to set the executed query plan
     * @param executedQueryPlan: the executed query plan for the query
     */
    void setExecutedQueryPlan(QueryPlanPtr executedQueryPlan);

    /**
     * @brief method to get the status of the query
     * @return query status
     */
    [[nodiscard]] QueryStatus::Value getQueryStatus() const;

    /**
     * @brief method to get the status of the query as string
     * @return query status: as string
     */
    [[nodiscard]] std::string getQueryStatusAsString() const;

    /**
     * @brief method to set the status of the query
     * @param query status
     */
    void setQueryStatus(QueryStatus::Value queryStatus);

    /**
     * @brief Get name of the query placement strategy
     * @return query placement strategy
     */
    [[nodiscard]] const std::string& getQueryPlacementStrategyAsString() const;

    /**
     * @brief Return placement strategy used for the query
     * @return queryPlacement strategy
     */
    PlacementStrategy::Value getQueryPlacementStrategy();

    void setMetaInformation(std::string metaInformation);

    std::string getMetaInformation();

    /**
     * @brief Adds a new phase to the optimizationPhases map
     * @param phaseName
     * @param queryPlan
     */
    void addOptimizationPhase(std::string phaseName, QueryPlanPtr queryPlan);

    /**
     * @brief Get all optimization phases for this query
     * @return
     */
    std::map<std::string, QueryPlanPtr> getOptimizationPhases();

    /**
     * Add sub query plan to the query catalog
     * @param querySubPlanId : the sub query plan id
     * @param workerId : the worker node on which the query is running
     */
    void addQuerySubPlanMetaData(QuerySubPlanId querySubPlanId, uint64_t workerId);

    /**
     * Get sub query plan meta data
     * @param querySubPlanId : the sub query plan id
     */
    QuerySubPlanMetaDataPtr getQuerySubPlanMetaData(QuerySubPlanId querySubPlanId);

    /**
     * Get all sub query plan mea data
     * @return vector of sub query plan meta data
     */
    std::vector<QuerySubPlanMetaDataPtr> getAllSubQueryPlanMetaData();

    void removeAllQuerySubPlanMetaData();

  private:
    mutable std::mutex mutex;
    QueryId queryId;
    std::string queryString;
    std::string queryPlacementStrategy;
    QueryPlanPtr inputQueryPlan;
    QueryPlanPtr executedQueryPlan;
    QueryStatus::Value queryStatus;
    std::string metaInformation;
    std::map<std::string, QueryPlanPtr> optimizationPhases;
    std::map<QuerySubPlanId, QuerySubPlanMetaDataPtr> querySubPlanMetaDataMap;
};
using QueryCatalogEntryPtr = std::shared_ptr<QueryCatalogEntry>;
}// namespace Catalogs::Query
}// namespace NES

#endif// NES_CORE_INCLUDE_CATALOGS_QUERY_QUERYCATALOGENTRY_HPP_

/**********************************************************************
Copyright (c) 2024 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.neo4j;

import java.util.List;
import java.util.Map;
import org.datanucleus.store.neo4j.query.LazyLoadQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

/**
 * Executes Cypher queries against an embedded Neo4j database instance.
 */
public final class EmbeddedQueryEngine {

    private EmbeddedQueryEngine() {
        // Private constructor
    }

    /**
     * Executes a parameterized Cypher query.
     */
    @SuppressWarnings("rawtypes")
    public static List executeCypherQuery(Query query, GraphDatabaseService db, String cypherText, Map<String, Object> parameters) {
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
            NucleusLogger.DATASTORE_NATIVE.debug("Executing Cypher query: " + cypherText);
            if (parameters != null && !parameters.isEmpty()) {
                NucleusLogger.DATASTORE_NATIVE.debug("With parameters: " + parameters);
            }
        }

        int resultStart = cypherText.toUpperCase().indexOf("RETURN ") + 7;
        String resultStr = cypherText.substring(resultStart);
        // Basic parsing to find the end of the RETURN clause
        int orderByStart = resultStr.toUpperCase().indexOf(" ORDER BY");
        if (orderByStart > 0) resultStr = resultStr.substring(0, orderByStart);
        int skipStart = resultStr.toUpperCase().indexOf(" SKIP");
        if (skipStart > 0) resultStr = resultStr.substring(0, skipStart);
        int limitStart = resultStr.toUpperCase().indexOf(" LIMIT");
        if (limitStart > 0) resultStr = resultStr.substring(0, limitStart);
        
        if (query.getCompilation() != null && query.getCompilation().getCandidateAlias() != null &&
            resultStr.trim().equals(query.getCompilation().getCandidateAlias())) {
            resultStr = null; // Indicates returning the candidate itself
        }

        Result queryResult = db.execute(cypherText, parameters);

        if (query.getExecutionContext().getStatistics() != null) {
            query.getExecutionContext().getStatistics().incrementNumReads();
        }

        return new LazyLoadQueryResult(query, queryResult, resultStr);
    }
}
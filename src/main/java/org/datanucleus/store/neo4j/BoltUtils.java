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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.neo4j.fieldmanager.RemoteFetchFieldManager;
import org.datanucleus.store.query.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.types.Node;

/**
 * A set of utilities for handling remote Bolt connections.
 */
public class BoltUtils {

    // === FIX: The method now returns a generic Object to handle both Lists and Longs ===
    public static Object executeCypherQuery(Query query, Transaction tx, String cypherText, Map<String, Object> params) {
        Result result = tx.run(cypherText, params);
        
        // === FIX: The logic is now split based on whether a candidate class is set ===
        if (query.getCandidateClass() != null) {
            // This is a query that is expected to return entity objects.
            List<Object> results = new ArrayList<>();
            while (result.hasNext()) {
                Record record = result.next();
                Node node = record.get(0).asNode();
                Object pc = getObjectForNode(query.getExecutionContext(), query.getCandidateClass(), node);
                results.add(pc);
            }
            return results;
        } else {
            // This is a bulk update/delete or aggregation query.
            // JDO spec says bulk DELETE should return a long.
            // We consume the result to get the summary, which contains the counters.
            ResultSummary summary = result.consume();
            return (long) summary.counters().nodesDeleted();
        }
    }

    public static Object getObjectForNode(final ExecutionContext ec, Class<?> candidateClass, final Node node) {
        AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, ec.getClassLoaderResolver());
        long idValue = node.id();
        
        Object idForLookup;
        if (cmd.getIdentityType() == IdentityType.APPLICATION) {
            idForLookup = ec.newObjectId(candidateClass, idValue);
        } else { // DATASTORE identity
            idForLookup = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idValue);
        }
        
        Object pc = ec.findObject(idForLookup, true, false, null);
        final DNStateManager sm = ec.findStateManager(pc);

        final int[] fpMembers = ec.getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers();
        sm.loadFieldValues(new FieldValues() {
            public void fetchFields(DNStateManager sm) {
                sm.replaceFields(fpMembers, new RemoteFetchFieldManager(sm, node));
            }
            public void fetchNonLoadedFields(DNStateManager sm) {
                sm.replaceNonLoadedFields(fpMembers, new RemoteFetchFieldManager(sm, node));
            }
            public FetchPlan getFetchPlanForLoading() {
                return ec.getFetchPlan();
            }
        });

        return pc;
    }
}
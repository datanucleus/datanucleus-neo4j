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
**********************************************************************/
package org.datanucleus.store.neo4j;

// Required DataNucleus imports based on your provided interfaces and context
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityManager;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.state.DNStateManager;

// Required Neo4j driver imports
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

/**
 * Utility methods for persistence operations using the Bolt driver.
 */
public class BoltPersistenceUtils {

    /**
     * Retrieves the Neo4j Node for a given StateManager.
     * It first checks the in-transaction cache on the StateManager and, if not found, queries the database.
     * @param tx The active Neo4j transaction.
     * @param sm The StateManager for the object.
     * @return The corresponding Neo4j Node, or null if not found.
     */
    public static Node getPropertyContainerForStateManager(Transaction tx, DNStateManager sm) {
        Object associatedValue = sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (associatedValue instanceof Node) {
            return (Node) associatedValue;
        }

        Object id = sm.getInternalObjectId();
        if (id == null) {
            return null;
        }

        if (sm.getClassMetaData().getIdentityType() == IdentityType.APPLICATION) {
            AbstractClassMetaData cmd = sm.getClassMetaData();
            String[] pkMemberNames = cmd.getPrimaryKeyMemberNames();
            String pkMemberName = pkMemberNames[0]; // Assuming single field PK for simplicity

            String cypher = String.format("MATCH (n:`%s` {`%s`: $pkValue}) RETURN n", cmd.getName(), pkMemberName);
            Object pkValue = IdentityUtils.getTargetKeyForSingleFieldIdentity(id);

            Result result = tx.run(cypher, Values.parameters("pkValue", pkValue));
            return result.hasNext() ? result.single().get("n").asNode() : null;
        }

        Long nodeId = (Long) IdentityUtils.getTargetKeyForDatastoreIdentity(id);
        if (nodeId == null) {
            return null;
        }

        Result result = tx.run("MATCH (n) WHERE id(n) = $id RETURN n", Values.parameters("id", nodeId));
        return result.hasNext() ? result.single().get("n").asNode() : null;
    }

    /**
     * Finds or creates a managed Java object for a given Neo4j Node.
     * This method TRUSTS that the provided AbstractClassMetaData is the correct one for the Node.
     * The responsibility for providing the correct metadata lies with the calling query engine.
     * @param ec The ExecutionContext.
     * @param node The Neo4j Node.
     * @param cmd The metadata for the class of the Java object (assumed to be correct).
     * @return The managed Java object.
     */
    public static Object getObjectForNode(ExecutionContext ec, Node node, AbstractClassMetaData cmd) {
        if (cmd == null) {
            throw new NucleusDataStoreException("Cannot create object for Node. The provided ClassMetaData is null.");
        }

        // Get the IdentityManager by casting the NucleusContext. This is required for the provided API version.
        IdentityManager identityMgr = ((PersistenceNucleusContext) ec.getNucleusContext()).getIdentityManager();

        Object id;
        if (cmd.getIdentityType() == IdentityType.DATASTORE) {
            id = identityMgr.getDatastoreId(cmd.getFullClassName(), node.id());
        } 
        else if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            Class<?> pcType = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
            int[] pkMemberPositions = cmd.getPKMemberPositions();
            if (pkMemberPositions.length == 1) {
                String pkMemberName = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkMemberPositions[0]).getName();
                if (!node.containsKey(pkMemberName)) {
                     throw new NucleusUserException("Application identity field '" + pkMemberName + "' not found in query results for " + cmd.getName() + ".");
                }
                Object pkValue = node.get(pkMemberName).asObject();
                if (pkValue == null) {
                    throw new NucleusUserException("Application identity field '" + pkMemberName + "' is null in the database for " + cmd.getName() + ".");
                }
                id = identityMgr.getApplicationId(pcType, pkValue);
            } else {
                StringBuilder pkStr = new StringBuilder();
                for (int i = 0; i < pkMemberPositions.length; i++) {
                    String pkMemberName = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkMemberPositions[i]).getName();
                    Object pkValue = node.get(pkMemberName).asObject();
                    if (i > 0) pkStr.append('_');
                    pkStr.append(pkValue.toString());
                }
                id = identityMgr.getApplicationId(pcType, pkStr.toString());
            }
        }
        else 
        {
            throw new NucleusDataStoreException("IdentityType " + cmd.getIdentityType() + " not supported");
        }

        return ec.findObject(id, true, true, null);
    }
}
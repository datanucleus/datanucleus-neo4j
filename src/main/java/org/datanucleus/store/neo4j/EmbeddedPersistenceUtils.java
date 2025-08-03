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

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import java.util.Map;

/**
 * High-level persistence utility class for Neo4j, acting as a facade for identity and object lookups.
 * This class is for the EMBEDDED driver ONLY and contains no Bolt-driver dependencies.
 */
public final class EmbeddedPersistenceUtils {

    private EmbeddedPersistenceUtils() {
        // Private constructor
    }

    public static PropertyContainer getPropertyContainerForStateManager(GraphDatabaseService graphDB, DNStateManager sm) {
        Object val = sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (val != null) {
            return (PropertyContainer) val;
        }

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        PropertyContainer propObj = getPropertyContainerForObjectId(graphDB, ec, cmd, sm.getInternalObjectId());
        if (propObj != null) {
            sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
        }
        return propObj;
    }

    public static PropertyContainer getPropertyContainerForObjectId(GraphDatabaseService graphDB, ExecutionContext ec, AbstractClassMetaData cmd, Object id) {
        StoreManager storeMgr = ec.getStoreManager();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null) {
            storeMgr.manageClasses(clr, cmd.getFullClassName());
            sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();
        boolean isAttributedRelation = Neo4jSchemaUtils.classIsAttributedRelation(cmd);

        if (cmd.pkIsDatastoreAttributed(storeMgr)) {
            long key = -1L;
            if (cmd.getIdentityType() == IdentityType.DATASTORE) {
                key = (Long) IdentityUtils.getTargetKeyForDatastoreIdentity(id);
            } else if (cmd.getIdentityType() == IdentityType.APPLICATION && cmd.getPKMemberPositions().length == 1) {
                key = (Long) IdentityUtils.getTargetKeyForSingleFieldIdentity(id);
            }
            if (key != -1L) {
                return isAttributedRelation ? graphDB.getRelationshipById(key) : graphDB.getNodeById(key);
            }
        }

        String indexName = Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX;
        if ((isAttributedRelation && !graphDB.index().existsForRelationships(indexName)) ||
            (!isAttributedRelation && !graphDB.index().existsForNodes(indexName))) {
            return null; // Index doesn't exist, so object can't exist
        }

        StringBuilder cypher = new StringBuilder();
        String startNode = (isAttributedRelation ? "relationship:" : "node:") + indexName;
        cypher.append("START pc=").append(startNode).append("(`").append(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY)
              .append("`=\"").append(cmd.getFullClassName()).append("\") WHERE ");

        if (cmd.getIdentityType() == IdentityType.APPLICATION) {
            int[] pkPositions = cmd.getPKMemberPositions();
            for (int i = 0; i < pkPositions.length; i++) {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                if (pkMmd.getRelationType(clr) != RelationType.NONE) {
                    throw new NucleusException("Retrieving objects with relation-based PK fields is not supported: " + pkMmd.getFullFieldName());
                }
                Object value = cmd.usesSingleFieldIdentityClass() ? IdentityUtils.getTargetKeyForSingleFieldIdentity(id) : IdentityUtils.getValueForMemberInId(id, pkMmd);
                Object storedValue = Neo4jPropertyManager.getStoredValueForField(ec, pkMmd, value, FieldRole.ROLE_FIELD);
                String propName = table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName();

                cypher.append("pc.`").append(propName).append("` = ").append(formatValueForCypher(storedValue));
                if (i < pkPositions.length - 1) {
                    cypher.append(" AND ");
                }
            }
        } else if (cmd.getIdentityType() == IdentityType.DATASTORE) {
            Object value = IdentityUtils.getTargetKeyForDatastoreIdentity(id);
            String propName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
            cypher.append("pc.`").append(propName).append("` = ").append(formatValueForCypher(value));
        } else {
            throw new NucleusException("Cannot retrieve object with non-durable identity.");
        }

        cypher.append(" RETURN pc");
        
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled()) {
            NucleusLogger.DATASTORE_NATIVE.debug("Retrieving object with Cypher: " + cypher.toString());
        }

        try (Result result = graphDB.execute(cypher.toString())) {
            if (!result.hasNext()) {
                return null;
            }
            Map<String, Object> row = result.next();
            PropertyContainer propObj = (PropertyContainer) row.get("pc");
            if (result.hasNext()) {
                throw new NucleusException("Found multiple objects for identity: " + id);
            }
            return propObj;
        }
    }

    private static String formatValueForCypher(Object value) {
        if (value instanceof String) {
            return "\"" + value + "\"";
        }
        return value.toString();
    }
    
    public static String getClassNameForIdentity(Object id, AbstractClassMetaData rootCmd, ExecutionContext ec, ClassLoaderResolver clr) {
        StoreManager storeMgr = ec.getStoreManager();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try {
            GraphDatabaseService db = (GraphDatabaseService) mconn.getConnection();
            PropertyContainer propObj = getPropertyContainerForObjectId(db, ec, rootCmd, id);
            if (propObj != null) {
                return getClassMetaDataForPropertyContainer(propObj, ec, rootCmd).getFullClassName();
            }
            return rootCmd.getFullClassName(); // Fallback
        } finally {
            mconn.release();
        }
    }

    public static AbstractClassMetaData getClassMetaDataForPropertyContainer(PropertyContainer propObj, ExecutionContext ec, AbstractClassMetaData cmd) {
        String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(cmd.getFullClassName(), false);
        if (subclassNames == null || subclassNames.length == 0) {
            return cmd;
        }

        if (propObj instanceof Relationship) {
            Index<Relationship> typesIdx = propObj.getGraphDatabase().index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX);
            for (String subclassName : subclassNames) {
                IndexHits<Relationship> hits = typesIdx.get(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, subclassName);
                if (hits != null) {
                    for (Relationship rel : hits) {
                        if (rel.equals(propObj)) {
                            return ec.getMetaDataManager().getMetaDataForClass(subclassName, ec.getClassLoaderResolver());
                        }
                    }
                }
            }
        } else {
            Index<Node> typesIdx = propObj.getGraphDatabase().index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX);
            for (String subclassName : subclassNames) {
                IndexHits<Node> hits = typesIdx.get(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, subclassName);
                 if (hits != null) {
                    for (Node node : hits) {
                        if (node.equals(propObj)) {
                            return ec.getMetaDataManager().getMetaDataForClass(subclassName, ec.getClassLoaderResolver());
                        }
                    }
                }
            }
        }
        return cmd;
    }
}
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
package org.datanucleus.store.neo4j.fieldmanager;

import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.types.Node;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class BoltRelationshipManager {

    private final Transaction tx;
    private final DNStateManager ownerSM;
    private final Node ownerNode;

    public BoltRelationshipManager(DNStateManager sm, Transaction tx, Node node) {
        this.ownerSM = sm;
        this.tx = tx;
        this.ownerNode = node;
    }

    public void storeRelationField(AbstractMemberMetaData mmd, Object relatedObject) {
        if (relatedObject == null) {
            return;
        }
        if (relatedObject instanceof Collection) {
            for (Object element : (Collection<?>) relatedObject) {
                createRelationship(mmd, element);
            }
        } else {
            createRelationship(mmd, relatedObject);
        }
    }

    private void createRelationship(AbstractMemberMetaData mmd, Object relatedObject) {
        ExecutionContext ec = ownerSM.getExecutionContext();
        DNStateManager relatedSM = ec.findStateManager(relatedObject);

        if (relatedSM == null || relatedSM.getLifecycleState().isNew()) {
            ec.persistObjectInternal(relatedObject, null, PersistableObjectType.PC, ownerSM, mmd.getAbsoluteFieldNumber());
            relatedSM = ec.findStateManager(relatedObject);
        }

        if (relatedSM == null) {
            throw new NucleusDataStoreException("Failed to find StateManager for related object: " + relatedObject);
        }

        // === THE DEFINITIVE FIX: Retrieve the Node directly from the cache ===
        Node relatedNode = (Node) relatedSM.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (relatedNode == null) {
            throw new NucleusDataStoreException("Could not find cached Node for related object: " + relatedSM.getObjectAsPrintable() +
                ". This indicates a failure in the persistence lifecycle.");
        }
        
        long ownerIdVal = this.ownerNode.id();
        long relatedIdVal = relatedNode.id();

        String relType = mmd.getName().toUpperCase();
        String cypher = String.format(
            "MATCH (a) WHERE id(a) = $ownerId " +
            "MATCH (b) WHERE id(b) = $relatedId " +
            "MERGE (a)-[:`%s`]->(b)",
            relType
        );
        
        Map<String, Object> params = new HashMap<>();
        params.put("ownerId", ownerIdVal);
        params.put("relatedId", relatedIdVal);

        tx.run(cypher, params).consume();
    }

    public void deleteRelationField(AbstractMemberMetaData mmd, Object relatedObject) {
        if (relatedObject == null) return;
        if (relatedObject instanceof Collection) {
            for (Object element : (Collection<?>) relatedObject) {
                deleteRelationship(mmd, element);
            }
        } else {
            deleteRelationship(mmd, relatedObject);
        }
    }

    private void deleteRelationship(AbstractMemberMetaData mmd, Object relatedObject) {
        if (mmd.isDependent()) {
            DNStateManager relatedSM = ownerSM.getExecutionContext().findStateManager(relatedObject);
            if (relatedSM == null || relatedSM.getInternalObjectId() == null) return;
            
            Object relatedIdVal = IdentityUtils.getTargetKeyForDatastoreIdentity(relatedSM.getInternalObjectId());
            String cypherNodeDelete = "MATCH (b) WHERE id(b) = $relatedId DETACH DELETE b";
            Map<String, Object> nodeDeleteParams = new HashMap<>();
            nodeDeleteParams.put("relatedId", relatedIdVal);
            tx.run(cypherNodeDelete, nodeDeleteParams);
        }
    }
}
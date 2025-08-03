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
package org.datanucleus.store.neo4j.fieldmanager;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.neo4j.fieldmanager.DNRelationshipType;
import org.datanucleus.store.neo4j.EmbeddedPersistenceUtils;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * Manages the persistence of relationships between Neo4j Nodes.
 */
public final class RelationshipManager {

    private RelationshipManager() {
        // Private constructor
    }

    public static void storeRelationField(DNStateManager sm, AbstractMemberMetaData mmd, Object value) {
        if (!(sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) instanceof Node)) {
            throw new NucleusUserException("Cannot create relationships from an object that is not persisted as a Node.");
        }
        Node ownerNode = (Node) sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();

        // === FIX #1: Use the correct API to check if the object is new ===
        // On update, clear out any old relationships for this member first.
        if (!sm.getExecutionContext().getApiAdapter().isNew(sm.getObject())) {
            deleteRelationshipsForMember(ownerNode, mmd);
        }

        if (value == null) {
            return; // No new relationships to create
        }
        
        RelationType relationType = mmd.getRelationType(clr);

        if (RelationType.isRelationSingleValued(relationType)) {
            processSingleValuedRelation(sm, ownerNode, mmd, value);
        } else if (RelationType.isRelationMultiValued(relationType)) {
            processMultiValuedRelation(sm, ownerNode, mmd, value);
        }
    }

    private static void processSingleValuedRelation(DNStateManager sm, Node ownerNode, AbstractMemberMetaData mmd, Object relatedObject) {
        ExecutionContext ec = sm.getExecutionContext();
        
        // === FIX #2: Pass null for the missing FieldValues argument ===
        DNStateManager relatedSM = ec.findStateManager(ec.persistObjectInternal(relatedObject, null, PersistableObjectType.PC, sm, mmd.getAbsoluteFieldNumber()));
        if (relatedSM == null) return;
        
        Node relatedNode = (Node) EmbeddedPersistenceUtils.getPropertyContainerForStateManager(ownerNode.getGraphDatabase(), relatedSM);
        if (relatedNode == null) return;

        Relationship rel = ownerNode.createRelationshipTo(relatedNode, DNRelationshipType.SINGLE_VALUED);
        populateRelationshipProperties(rel, mmd);
    }

    private static void processMultiValuedRelation(DNStateManager sm, Node ownerNode, AbstractMemberMetaData mmd, Object value) {
        ExecutionContext ec = sm.getExecutionContext();

        if (mmd.hasCollection()) {
            int index = 0;
            for (Object element : (Collection<?>) value) {
                // === FIX #2: Pass null for the missing FieldValues argument ===
                DNStateManager elementSM = ec.findStateManager(ec.persistObjectInternal(element, null, PersistableObjectType.PC, sm, mmd.getAbsoluteFieldNumber()));
                Node elementNode = (Node) EmbeddedPersistenceUtils.getPropertyContainerForStateManager(ownerNode.getGraphDatabase(), elementSM);
                Relationship rel = ownerNode.createRelationshipTo(elementNode, DNRelationshipType.MULTI_VALUED);
                populateRelationshipProperties(rel, mmd);
                if (value instanceof List) {
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, index++);
                }
            }
        } else if (mmd.hasArray()) {
            for (int i = 0; i < Array.getLength(value); i++) {
                Object element = Array.get(value, i);
                // === FIX #2: Pass null for the missing FieldValues argument ===
                DNStateManager elementSM = ec.findStateManager(ec.persistObjectInternal(element, null, PersistableObjectType.PC, sm, mmd.getAbsoluteFieldNumber()));
                Node elementNode = (Node) EmbeddedPersistenceUtils.getPropertyContainerForStateManager(ownerNode.getGraphDatabase(), elementSM);
                Relationship rel = ownerNode.createRelationshipTo(elementNode, DNRelationshipType.MULTI_VALUED);
                populateRelationshipProperties(rel, mmd);
                rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, i);
            }
        } else if (mmd.hasMap()) {
            throw new NucleusUserException("Map-based relations are not currently supported by this handler: " + mmd.getFullFieldName());
        }
    }

    private static void populateRelationshipProperties(Relationship rel, AbstractMemberMetaData mmd) {
        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
        
        ClassLoaderResolver clr = mmd.getAbstractClassMetaData().getMetaDataManager().getNucleusContext().getClassLoaderResolver(null);
        if (RelationType.isBidirectional(mmd.getRelationType(clr))) {
            AbstractMemberMetaData[] otherMmds = mmd.getRelatedMemberMetaData(clr);
            if (otherMmds != null && otherMmds.length > 0) {
                rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, otherMmds[0].getName());
            }
        }
    }

    private static void deleteRelationshipsForMember(Node ownerNode, AbstractMemberMetaData mmd) {
        for (Relationship rel : ownerNode.getRelationships()) {
            if (rel.hasProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME) &&
                rel.getProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME).equals(mmd.getName())) {
                rel.delete();
            }
        }
    }
}
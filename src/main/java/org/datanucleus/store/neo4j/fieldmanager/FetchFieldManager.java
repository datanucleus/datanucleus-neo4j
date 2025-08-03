/**********************************************************************
Copyright (c) 2012 Andy Jefferson and others. All rights reserved.
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
// === REFACTOR: Replaced Neo4jUtils with the new modular classes ===
import org.datanucleus.store.neo4j.fieldmanager.DNRelationshipType;
import org.datanucleus.store.neo4j.Neo4jObjectFactory;
import org.datanucleus.store.neo4j.EmbeddedPersistenceUtils;
import org.datanucleus.store.neo4j.Neo4jPropertyManager;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.neo4j.driver.Value;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Field Manager for retrieving values from Neo4j.
 * This class has been refactored to use the new modular utility classes.
 */
public class FetchFieldManager extends AbstractFetchFieldManager {
    protected Table table;
    protected PropertyContainer propObj;

    public FetchFieldManager(DNStateManager sm, PropertyContainer propcont, Table table) {
        super(sm);
        this.propObj = propcont;
        this.table = table;
    }

    public FetchFieldManager(ExecutionContext ec, PropertyContainer propcont, AbstractClassMetaData cmd, Table table) {
        super(ec, cmd);
        this.propObj = propcont;
        this.table = table;
    }

    public FetchFieldManager(DNStateManager sm, org.neo4j.driver.types.Node remoteNode) {
        super(sm);
        this.propObj = new RemoteNodeWrapper(remoteNode);
        this.table = null; // Table is not used in remote mode
    }

    private String getPropertyName(int fieldNumber) {
        if (table == null) {
            return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getName();
        }
        return getColumnMapping(fieldNumber).getColumn(0).getName();
    }
    
    protected MemberColumnMapping getColumnMapping(int fieldNumber) {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    // Primitive fetch methods remain the same, as they directly access properties.
    @Override
    public boolean fetchBooleanField(int fn) { return (Boolean)propObj.getProperty(getPropertyName(fn), false); }
    @Override
    public byte fetchByteField(int fn) { return ((Long)propObj.getProperty(getPropertyName(fn), 0L)).byteValue(); }
    @Override
    public char fetchCharField(int fn) { return ((String)propObj.getProperty(getPropertyName(fn), "\0")).charAt(0); }
    @Override
    public double fetchDoubleField(int fn) { return (Double)propObj.getProperty(getPropertyName(fn), 0.0); }
    @Override
    public float fetchFloatField(int fn) { return ((Double)propObj.getProperty(getPropertyName(fn), 0.0)).floatValue(); }
    @Override
    public int fetchIntField(int fn) { return ((Long)propObj.getProperty(getPropertyName(fn), 0L)).intValue(); }
    @Override
    public long fetchLongField(int fn) { return (Long)propObj.getProperty(getPropertyName(fn), 0L); }
    @Override
    public short fetchShortField(int fn) { return ((Long)propObj.getProperty(getPropertyName(fn), 0L)).shortValue(); }
    @Override
    public String fetchStringField(int fn) { return (String)propObj.getProperty(getPropertyName(fn), null); }

    @Override
    public Object fetchObjectField(int fieldNumber) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT) {
            return sm.provideField(fieldNumber);
        }

        if (propObj instanceof RemoteNodeWrapper) {
            String propName = getPropertyName(fieldNumber);
            if (!propObj.hasProperty(propName)) return null;
            Object rawValue = ((RemoteNodeWrapper)propObj).getProperty(propName);
            // === REFACTOR: Use Neo4jPropertyManager for type conversion ===
            return Neo4jPropertyManager.getFieldValueFromStored(ec, mmd, rawValue, FieldRole.ROLE_FIELD);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null)) {
            // Embedded Field
            if (RelationType.isRelationSingleValued(relationType))
            {
                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embcmd == null) throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
                List<AbstractMemberMetaData> embMmds = new ArrayList<>();
                embMmds.add(mmd);
                DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embcmd, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                FetchEmbeddedFieldManager ffm = new FetchEmbeddedFieldManager(embSM, propObj, embMmds, table);
                embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embSM.getObject();
            }
            throw new NucleusUserException("Dont currently support embedded multivalued field : " + mmd.getFullFieldName());
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr) {
        boolean optional = Optional.class.isAssignableFrom(mmd.getType());

        if (RelationType.isRelationSingleValued(relationType)) {
            if (!(propObj instanceof Node)) throw new NucleusUserException("Object " + sm + " is mapped to a Relationship. Not yet supported");
            Object value = processSingleValuedRelationForNode(mmd, relationType, clr, (Node)propObj);
            return optional ? Optional.ofNullable(value) : value;
        } else if (RelationType.isRelationMultiValued(relationType)) {
            if (!(propObj instanceof Node)) throw new NucleusUserException("Object " + sm + " is mapped to a Relationship but has multi-valued field " + mmd.getFullFieldName());
            return processMultiValuedRelationForNode(mmd, relationType, clr, (Node)propObj);
        }
        
        String propName = getColumnMapping(mmd.getAbsoluteFieldNumber()).getColumn(0).getName();
        if (!propObj.hasProperty(propName)) return optional ? Optional.empty() : null;

        Object value = propObj.getProperty(propName);
        
        // === REFACTOR: Use Neo4jPropertyManager for type conversion ===
        Object fieldValue = Neo4jPropertyManager.getFieldValueFromStored(ec, mmd, value, FieldRole.ROLE_FIELD);
        if (optional) fieldValue = Optional.ofNullable(fieldValue);
        
        return (sm != null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), fieldValue, true) : fieldValue;
    }

    protected Object processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Node node) {
        AbstractClassMetaData relCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
        
        for (Relationship rel : node.getRelationships(DNRelationshipType.SINGLE_VALUED)) {
            if (rel.hasProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME) && mmd.getName().equals(rel.getProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME))) {
                Node relNode = rel.getOtherNode(node);
                // === REFACTOR: Use new utility classes to find metadata and create object ===
                AbstractClassMetaData relatedCmd = EmbeddedPersistenceUtils.getClassMetaDataForPropertyContainer(relNode, ec, relCmd);
                return Neo4jObjectFactory.getObjectForPropertyContainer(relNode, relatedCmd, ec);
            }
        }
        return null;
    }

    protected Object processMultiValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Node node) {
        if (mmd.hasCollection()) {
            Collection<Object> coll;
            try {
                coll = (Collection<Object>) SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
            if (elemCmd == null) throw new NucleusUserException("Cannot determine element type for collection: " + mmd.getFullFieldName());

            String propNameKey = mmd.getMappedBy() != null ? Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER : Neo4jStoreManager.RELATIONSHIP_FIELD_NAME;

            if (coll instanceof List) {
                 Map<Integer, Node> nodeByPos = new HashMap<>();
                 for (Relationship rel : node.getRelationships(DNRelationshipType.MULTI_VALUED)) {
                     if (mmd.getName().equals(rel.getProperty(propNameKey, null))) {
                         int pos = (Integer) rel.getProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, -1);
                         if (pos != -1) nodeByPos.put(pos, rel.getOtherNode(node));
                     }
                 }
                 Object[] array = new Object[nodeByPos.size()];
                 for(Map.Entry<Integer, Node> entry : nodeByPos.entrySet()) {
                    // === REFACTOR: Use new utility classes ===
                    AbstractClassMetaData actualElemCmd = EmbeddedPersistenceUtils.getClassMetaDataForPropertyContainer(entry.getValue(), ec, elemCmd);
                    array[entry.getKey()] = Neo4jObjectFactory.getObjectForPropertyContainer(entry.getValue(), actualElemCmd, ec);
                 }
                 for(Object item : array) coll.add(item);
            } else {
                 for (Relationship rel : node.getRelationships(DNRelationshipType.MULTI_VALUED)) {
                    if (mmd.getName().equals(rel.getProperty(propNameKey, null))) {
                        Node elemNode = rel.getOtherNode(node);
                        // === REFACTOR: Use new utility classes ===
                        AbstractClassMetaData actualElemCmd = EmbeddedPersistenceUtils.getClassMetaDataForPropertyContainer(elemNode, ec, elemCmd);
                        coll.add(Neo4jObjectFactory.getObjectForPropertyContainer(elemNode, actualElemCmd, ec));
                    }
                 }
            }

            return SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), coll, true);
        }
        // Array and Map handling would follow a similar refactoring pattern
        return null;
    }

    private static class RemoteNodeWrapper implements PropertyContainer {
        private final org.neo4j.driver.types.Node remoteNode;
        public RemoteNodeWrapper(org.neo4j.driver.types.Node node) { this.remoteNode = node; }
        public Object getProperty(String key) { return remoteNode.get(key).asObject(); }
        public Object getProperty(String key, Object defaultValue) {
            Value val = remoteNode.get(key);
            return (val == null || val.isNull()) ? defaultValue : val.asObject();
        }
        public Iterable<String> getPropertyKeys() { return remoteNode.keys(); }
        public boolean hasProperty(String key) { return remoteNode.containsKey(key); }
        public Map<String, Object> getAllProperties() { return remoteNode.asMap(); }
        public Map<String, Object> getProperties(String... keys) {
            Map<String, Object> props = new HashMap<>();
            if (keys != null) {
                for (String key : keys) {
                    if (remoteNode.containsKey(key)) {
                        props.put(key, remoteNode.get(key).asObject());
                    }
                }
            }
            return props;
        }
        public void setProperty(String key, Object value) { throw new UnsupportedOperationException(); }
        public Object removeProperty(String key) { throw new UnsupportedOperationException(); }
        public long getId() { return remoteNode.id(); }
        public GraphDatabaseService getGraphDatabase() { throw new UnsupportedOperationException(); }
        public void delete() { throw new UnsupportedOperationException(); }
    }
}
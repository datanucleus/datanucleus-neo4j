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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.neo4j.BoltPersistenceUtils;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

public class BoltFetchFieldManager extends AbstractFetchFieldManager {
    private final Transaction tx;
    private final Node node;

    public BoltFetchFieldManager(DNStateManager sm, Transaction tx, Node node) {
        super(sm);
        this.tx = tx;
        this.node = node;
    }

    @Override
    public Object fetchObjectField(int fieldNumber) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (relationType == RelationType.NONE) {
            // This field is a simple property
            if (node.containsKey(mmd.getName())) {
                Value val = node.get(mmd.getName());
                return val.isNull() ? null : val.asObject();
            }
            return null;
        }

        // This field is a relationship
        if (RelationType.isRelationSingleValued(relationType)) {
            return fetchSingleValuedRelation(mmd);
        } else if (RelationType.isRelationMultiValued(relationType)) {
            return fetchMultiValuedRelation(mmd);
        }
        return null; // Should not be reached
    }

    private Object fetchSingleValuedRelation(AbstractMemberMetaData mmd) {
        String relType = mmd.getName().toUpperCase();
        String cypher = String.format("MATCH (n)-[:%s]->(m) WHERE id(n) = $id RETURN m LIMIT 1", relType);
        
        Map<String, Object> params = new HashMap<>();
        params.put("id", node.id());
        Result result = tx.run(cypher, params);
        
        if (result.hasNext()) {
            Node relatedNode = result.next().get("m").asNode();
            AbstractClassMetaData relatedCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), ec.getClassLoaderResolver());
            return BoltPersistenceUtils.getObjectForNode(ec, relatedNode, relatedCmd);
        }
        return null;
    }
    
    private Object fetchMultiValuedRelation(AbstractMemberMetaData mmd) {
        if (!mmd.hasCollection() && !mmd.hasMap()) {
            return null;
        }

        // ============================ THE DEFINITIVE FIX ============================
        // Instantiate the correct Collection type (Set, List, etc.) based on the field metadata.
        Collection<Object> coll;
        try {
            Class<?> collClass = mmd.getType();
            if (collClass.isInterface()) {
                // If it's an interface (e.g., java.util.Set), use a default implementation.
                if (Set.class.isAssignableFrom(collClass)) {
                    coll = new HashSet<>();
                } else { // Default to ArrayList for List, Collection, etc.
                    coll = new ArrayList<>();
                }
            } else {
                // If it's a concrete class (e.g., java.util.ArrayList), instantiate it directly.
                coll = (Collection<Object>) collClass.getDeclaredConstructor().newInstance();
            }
        } catch (Exception e) {
            // Fallback to ArrayList if custom collection instantiation fails
            coll = new ArrayList<>();
        }
        // ========================== END OF THE DEFINITIVE FIX ========================

        AbstractClassMetaData elementCmd = mmd.getCollection().getElementClassMetaData(ec.getClassLoaderResolver());
        String relType = mmd.getName().toUpperCase();
        String cypher = String.format("MATCH (n)-[:%s]->(m) WHERE id(n) = $id RETURN m", relType);
        
        Map<String, Object> params = new HashMap<>();
        params.put("id", node.id());
        Result result = tx.run(cypher, params);
        
        List<Record> records = result.list();
        for (Record record : records) {
            Node relatedNode = record.get("m").asNode();
            coll.add(BoltPersistenceUtils.getObjectForNode(ec, relatedNode, elementCmd));
        }
        return coll;
    }
    
    @Override public boolean fetchBooleanField(int fn) { return node.get(getMMD(fn).getName()).asBoolean(); }
    @Override public byte fetchByteField(int fn) { return (byte) node.get(getMMD(fn).getName()).asLong(); }
    @Override public char fetchCharField(int fn) { return node.get(getMMD(fn).getName()).asString().charAt(0); }
    @Override public double fetchDoubleField(int fn) { return node.get(getMMD(fn).getName()).asDouble(); }
    @Override public float fetchFloatField(int fn) { return node.get(getMMD(fn).getName()).asFloat(); }
    @Override public int fetchIntField(int fn) { return node.get(getMMD(fn).getName()).asInt(); }
    @Override public long fetchLongField(int fn) { return node.get(getMMD(fn).getName()).asLong(); }
    @Override public short fetchShortField(int fn) { return (short) node.get(getMMD(fn).getName()).asLong(); }
    @Override public String fetchStringField(int fn) { return node.get(getMMD(fn).getName()).asString(); }
    private AbstractMemberMetaData getMMD(int fn) { return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fn); }
}
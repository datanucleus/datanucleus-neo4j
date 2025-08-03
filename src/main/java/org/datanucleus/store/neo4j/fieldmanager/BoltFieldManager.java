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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.neo4j.Neo4jSchemaUtils;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.Node;

public class BoltFieldManager extends AbstractStoreFieldManager {

    private final Transaction tx;
    private final Node node; // Used for deletes/updates

    private Map<String, Object> properties;
    private Map<AbstractMemberMetaData, Object> relationFields;

    public BoltFieldManager(DNStateManager sm, Transaction tx, boolean insert) {
        super(sm, insert);
        this.tx = tx;
        this.node = null;
        if (insert) {
            this.properties = new HashMap<>();
            this.relationFields = new HashMap<>();
        }
    }

    public BoltFieldManager(DNStateManager sm, Transaction tx, boolean insert, Node node) {
        super(sm, insert);
        this.tx = tx;
        this.node = node;
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value) {
        if (!isStorable(fieldNumber)) {
            return;
        }
        AbstractMemberMetaData mmd = getMMD(fieldNumber);
        ClassLoaderResolver clr = sm.getExecutionContext().getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        if (insert) {
            if (relationType != RelationType.NONE && !mmd.isEmbedded()) {
                if (value != null) {
                    relationFields.put(mmd, value);
                }
            } else {
                properties.put(mmd.getName(), value);
            }
        } else { // Delete operation
            if (relationType != RelationType.NONE && !mmd.isEmbedded() && value != null) {
                BoltRelationshipManager relMgr = new BoltRelationshipManager(sm, tx, this.node);
                relMgr.deleteRelationField(mmd, value);
            }
        }
    }

    @Override public void storeBooleanField(int fn, boolean v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeCharField(int fn, char v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),String.valueOf(v)); }
    @Override public void storeByteField(int fn, byte v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeShortField(int fn, short v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeIntField(int fn, int v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeLongField(int fn, long v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeFloatField(int fn, float v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeDoubleField(int fn, double v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }
    @Override public void storeStringField(int fn, String v) { if(insert && isStorable(fn)) properties.put(getMMD(fn).getName(),v); }

    public void execute() {
        if (!insert) {
            return;
        }

        List<String> labelList = Neo4jSchemaUtils.getLabelsForClass(cmd, sm.getExecutionContext().getStoreManager());
        StringBuilder cypher = new StringBuilder("CREATE (n");
        labelList.forEach(label -> cypher.append(":").append(Neo4jSchemaUtils.getLabelName(label)));
        cypher.append(" $props) RETURN n");

        Result result = tx.run(cypher.toString(), Values.parameters("props", properties));
        Node createdNode;
        if (result.hasNext()) {
            Record record = result.next();
            createdNode = record.get(0).asNode();
            
            // CRITICAL: Cache the node immediately so recursive calls can find it.
            sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, createdNode);

            if (cmd.getIdentityType() == org.datanucleus.metadata.IdentityType.DATASTORE) {
                sm.setPostStoreNewObjectId(createdNode.id());
            }
        } else {
            throw new NucleusDataStoreException("CREATE query failed to return the created node for: " + sm.getObjectAsPrintable());
        }

        if (!relationFields.isEmpty()) {
            BoltRelationshipManager relMgr = new BoltRelationshipManager(sm, tx, createdNode);
            for (Map.Entry<AbstractMemberMetaData, Object> entry : relationFields.entrySet()) {
                relMgr.storeRelationField(entry.getKey(), entry.getValue());
            }
        }
    }
    
    private AbstractMemberMetaData getMMD(int fn) { 
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fn); 
    }
}
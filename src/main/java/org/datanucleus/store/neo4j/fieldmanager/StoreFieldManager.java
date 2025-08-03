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

import java.util.Optional;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.neo4j.Neo4jPropertyManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.neo4j.graphdb.PropertyContainer;

/**
 * Field Manager for putting values from a POJO into a Neo4j PropertyContainer.
 * This class has been refactored to be a high-level orchestrator.
 */
public class StoreFieldManager extends AbstractStoreFieldManager {

    protected Table table;
    // === FIX: Made protected so subclasses can access it ===
    protected PropertyContainer propContainer;

    public StoreFieldManager(DNStateManager sm, PropertyContainer propContainer, boolean insert, Table table) {
        super(sm, insert);
        this.table = table;
        this.propContainer = propContainer;
    }

    // === FIX: Added missing constructor that the subclass needs ===
    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, PropertyContainer propContainer, boolean insert, Table table) {
        super(ec, cmd, insert);
        this.table = table;
        this.propContainer = propContainer;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber) {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    protected void storeProperty(int fieldNumber, Object value) {
        if (!isStorable(fieldNumber)) {
            return;
        }
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        Object storedValue = Neo4jPropertyManager.getStoredValueForField(ec, mmd, value, FieldRole.ROLE_FIELD);
        
        String propName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (storedValue == null) {
            if (!insert && propContainer.hasProperty(propName)) {
                propContainer.removeProperty(propName);
            }
        } else {
            propContainer.setProperty(propName, storedValue);
        }
    }
    
    @Override
    public void storeBooleanField(int fn, boolean v) { storeProperty(fn, v); }
    @Override
    public void storeCharField(int fn, char v) { storeProperty(fn, String.valueOf(v)); }
    @Override
    public void storeByteField(int fn, byte v) { storeProperty(fn, v); }
    @Override
    public void storeShortField(int fn, short v) { storeProperty(fn, v); }
    @Override
    public void storeIntField(int fn, int v) { storeProperty(fn, v); }
    @Override
    public void storeLongField(int fn, long v) { storeProperty(fn, v); }
    @Override
    public void storeFloatField(int fn, float v) { storeProperty(fn, v); }
    @Override
    public void storeDoubleField(int fn, double v) { storeProperty(fn, v); }
    @Override
    public void storeStringField(int fn, String v) { storeProperty(fn, v); }

    @Override
    public void storeObjectField(int fieldNumber, Object value) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd)) return;

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        
        if (value instanceof Optional) {
            value = ((Optional<?>) value).orElse(null);
        }
        
        if (relationType != RelationType.NONE) {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null)) {
                EmbeddedManager.storeEmbeddedField(sm, mmd, value);
            } else {
                RelationshipManager.storeRelationField(sm, mmd, value);
            }
        } else {
            storeProperty(fieldNumber, value);
        }
        
        SCOUtils.wrapSCOField(sm, fieldNumber, value, true);
    }
}
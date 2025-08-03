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

import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.fieldmanager.FetchFieldManager;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.neo4j.graphdb.PropertyContainer;

/**
 * Responsible for creating and hydrating POJOs from Neo4j PropertyContainer results.
 */
public final class Neo4jObjectFactory {

    private Neo4jObjectFactory() {
        // Private constructor
    }

    /**
     * Creates a POJO from a Neo4j PropertyContainer.
     */
    public static Object getObjectForPropertyContainer(PropertyContainer propObj, AbstractClassMetaData cmd,
            ExecutionContext ec) {
        final int[] fpMembers = ec.getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers();

        if (cmd.getIdentityType() == IdentityType.APPLICATION) {
            return getObjectWithApplicationId(propObj, cmd, ec, fpMembers);
        } else if (cmd.getIdentityType() == IdentityType.DATASTORE) {
            return getObjectWithDatastoreId(propObj, cmd, ec, fpMembers);
        } else {
            return getObjectWithNondurableId(propObj, cmd, ec, fpMembers);
        }
    }

    private static Object getObjectWithApplicationId(final PropertyContainer propObj, final AbstractClassMetaData cmd,
            final ExecutionContext ec, final int[] fpMembers) {
        
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        Table table = (sd != null) ? sd.getTable() : null;
        
        final FieldManager fm = new FetchFieldManager(ec, propObj, cmd, table);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);
        
        Class<?> type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        DNStateManager sm = ec.findStateManager(pc);
        
        // If the object was newly instantiated, we need to load its fields.
        if (sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null) {
            loadObjectState(sm, propObj, fpMembers, fm, table);
        }
        
        return pc;
    }

    private static Object getObjectWithDatastoreId(final PropertyContainer propObj, final AbstractClassMetaData cmd,
            final ExecutionContext ec, final int[] fpMembers) {
        
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        Table table = (sd != null) ? sd.getTable() : null;
        Object idKey = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName());

        Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);
        Class<?> type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        DNStateManager sm = ec.findStateManager(pc);
        
        if (sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null) {
            final FieldManager fm = new FetchFieldManager(sm, propObj, table);
            loadObjectState(sm, propObj, fpMembers, fm, table);
        }
        
        return pc;
    }

    private static Object getObjectWithNondurableId(final PropertyContainer propObj, final AbstractClassMetaData cmd,
            final ExecutionContext ec, final int[] fpMembers) {
        
        SCOID id = new SCOID(cmd.getFullClassName());
        Class<?> type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        DNStateManager sm = ec.findStateManager(pc);
        
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        Table table = (sd != null) ? sd.getTable() : null;

        if (sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null) {
            final FieldManager fm = new FetchFieldManager(sm, propObj, table);
            loadObjectState(sm, propObj, fpMembers, fm, table);
        }
        
        return pc;
    }

    private static void loadObjectState(DNStateManager sm, final PropertyContainer propObj, final int[] fpMembers, final FieldManager fm, final Table table) {
        sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
        
        sm.loadFieldValues(new FieldValues() {
            public void fetchFields(DNStateManager sm) {
                sm.replaceFields(fpMembers, fm);
            }
            public void fetchNonLoadedFields(DNStateManager sm) {
                sm.replaceNonLoadedFields(fpMembers, fm);
            }
            public FetchPlan getFetchPlanForLoading() {
                return sm.getExecutionContext().getFetchPlan();
            }
        });

        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.isVersioned()) {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            Object version;
            if (vermd.getMemberName() != null) {
                version = sm.provideField(cmd.getMetaDataForMember(vermd.getMemberName()).getAbsoluteFieldNumber());
            } else {
                version = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
            }
            sm.setVersion(version);
        }

        // Wrap any newly loaded SCO fields
        sm.replaceAllLoadedSCOFieldsWithWrappers();
    }
}
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
package org.datanucleus.store.neo4j.embedded;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.exceptions.NucleusOptimisticException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.metadata.VersionStrategy;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.DeleteFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.EmbeddedPersistenceUtils;
import org.datanucleus.store.neo4j.Neo4jSchemaUtils;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.datanucleus.store.neo4j.fieldmanager.FetchFieldManager;
import org.datanucleus.store.neo4j.fieldmanager.StoreFieldManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * Persistence Handler for embedded Neo4j.
 * This class has been refactored to use the new modular utility classes.
 */
public class EmbeddedPersistenceHandler extends AbstractPersistenceHandler
{
    public EmbeddedPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    @Override
    public void close()
    {
        // No resources to close
    }

    @Override
    public void insertObjects(DNStateManager... sms)
    {
        ExecutionContext ec = sms[0].getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.InsertObjects.Start", StringUtils.objectArrayToString(sms)));
            }

            for (DNStateManager sm : sms)
            {
                insertObjectToPropertyContainer(sm, db);
            }

            for (DNStateManager sm : sms)
            {
                AbstractClassMetaData cmd = sm.getClassMetaData();
                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                Table table = sd.getTable();
                PropertyContainer propObj = (PropertyContainer)sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
                int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
                if (relPositions.length > 0)
                {
                    sm.provideFields(relPositions, new StoreFieldManager(sm, propObj, true, table));
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                // === FIX START ===
                for (int i=0; i < sms.length; i++)
                {
                    ec.getStatistics().incrementInsertCount();
                }
                // === FIX END ===
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting objects", e);
            throw new NucleusDataStoreException("Exception inserting objects", e);
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void insertObject(DNStateManager sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
 
            PropertyContainer propObj = insertObjectToPropertyContainer(sm, db);
            
            AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            Table table = (sd != null) ? sd.getTable() : null;
            int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
            if (relPositions.length > 0)
            {
                sm.provideFields(relPositions, new StoreFieldManager(sm, propObj, true, table));
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }
    
    @Override
    public void updateObject(DNStateManager sm, int[] fieldNumbers)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
            AbstractClassMetaData cmd = sm.getClassMetaData();

            PropertyContainer propObj = EmbeddedPersistenceUtils.getPropertyContainerForStateManager(db, sm);

            if (propObj == null)
            {
                throw new NucleusDataStoreException("Could not find object with id " + sm.getInternalObjectId() + " to update.");
            }
            
            if (cmd.isVersioned())
            {
                // Note: Optimistic lock checking should be handled by the LockManager/StoreManager before this point
                Object currentVersion = sm.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                sm.setTransactionalVersion(nextVersion);
            }
            
            Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
            sm.provideFields(fieldNumbers, new StoreFieldManager(sm, propObj, false, table));

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void deleteObject(DNStateManager sm)
    {
        assertReadOnlyForUpdateOfObject(sm);

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
            PropertyContainer propObj = EmbeddedPersistenceUtils.getPropertyContainerForStateManager(db, sm);

            if (propObj == null)
            {
                return; // Already deleted
            }

            sm.loadUnloadedFields();
            int[] relMemberPosns = sm.getClassMetaData().getRelationMemberPositions(ec.getClassLoaderResolver());
            sm.provideFields(relMemberPosns, new DeleteFieldManager(sm, true));

            if (propObj instanceof Node)
            {
                Node node = (Node) propObj;
                for (Relationship rel : node.getRelationships()) {
                    rel.delete();
                }
                db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).remove(node);
                node.delete();
            }
            else if (propObj instanceof Relationship)
            {
                db.index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).remove((Relationship)propObj);
                ((Relationship)propObj).delete();
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void fetchObject(DNStateManager sm, int[] fieldNumbers)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
            PropertyContainer propObj = EmbeddedPersistenceUtils.getPropertyContainerForStateManager(db, sm);
            
            if (propObj == null)
            {
                throw new NucleusObjectNotFoundException("Datastore object not found for id: " + sm.getInternalObjectId());
            }

            Table table = storeMgr.getStoreDataForClass(sm.getClassMetaData().getFullClassName()).getTable();
            sm.replaceFields(fieldNumbers, new FetchFieldManager(sm, propObj, table));

            if (sm.getClassMetaData().isVersioned() && sm.getTransactionalVersion() == null)
            {
                VersionMetaData vermd = sm.getClassMetaData().getVersionMetaDataForClass();
                Object datastoreVersion;
                if (vermd.getMemberName() != null) {
                    datastoreVersion = sm.provideField(sm.getClassMetaData().getAbsolutePositionOfMember(vermd.getMemberName()));
                } else {
                    datastoreVersion = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                sm.setVersion(datastoreVersion);
            }
            
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void locateObject(DNStateManager sm)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
                PropertyContainer propObj = EmbeddedPersistenceUtils.getPropertyContainerForStateManager(db, sm);
                if (propObj == null)
                {
                    throw new NucleusObjectNotFoundException("Object not found for id: " + sm.getInternalObjectId());
                }
            }
            finally
            {
                mconn.release();
            }
        }
    }

    @Override
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }
    
    private PropertyContainer insertObjectToPropertyContainer(DNStateManager sm, GraphDatabaseService db)
    {
        assertReadOnlyForUpdateOfObject(sm);
        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        
        if ((cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE) &&
            !cmd.pkIsDatastoreAttributed(storeMgr))
        {
            try {
                locateObject(sm);
                throw new NucleusUserException(Localiser.msg("Neo4j.Insert.ObjectWithIdAlreadyExists", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            } catch (NucleusObjectNotFoundException onfe) {
                // Expected
            }
        }
        
        Table table = storeMgr.getStoreDataForClass(cmd.getFullClassName()).getTable();
        if (Neo4jSchemaUtils.classIsAttributedRelation(cmd)) {
             throw new NucleusUserException("Persisting objects as 'attributed relations' is not yet supported.");
        }
        PropertyContainer propObj = db.createNode();
        
        addPropertyContainerToTypeIndex(db, propObj, cmd, false);
        sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);

        if (cmd.pkIsDatastoreAttributed(storeMgr))
        {
            long nativeId = (propObj instanceof Node) ? ((Node)propObj).getId() : -1L;
            if (nativeId != -1) sm.setPostStoreNewObjectId(nativeId);
        }
        if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            String propName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
            Object key = IdentityUtils.getTargetKeyForDatastoreIdentity(sm.getInternalObjectId());
            propObj.setProperty(propName, key);
        }

        if (cmd.isVersioned())
        {
            VersionMetaData vermd = cmd.getVersionMetaDataForClass();
            if (vermd.getStrategy() == VersionStrategy.VERSION_NUMBER)
            {
                long version = 1L;
                sm.setTransactionalVersion(version);
                // === FIX START ===
                if (vermd.getMemberName() != null) {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getMemberName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), version);
                } else {
                    propObj.setProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName(), version);
                }
                // === FIX END ===
            }
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            propObj.setProperty(table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName(), cmd.getDiscriminatorValue());
        }
        Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
        if (multitenancyCol != null && ec.getTenantId() != null)
        {
            propObj.setProperty(multitenancyCol.getName(), ec.getTenantId());
        }

        int[] nonRelPositions = cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver());
        sm.provideFields(nonRelPositions, new StoreFieldManager(sm, propObj, true, table));

        return propObj;
    }

    private void addPropertyContainerToTypeIndex(GraphDatabaseService db, PropertyContainer propObj, AbstractClassMetaData cmd, boolean isSuperclass)
    {
        String className = cmd.getFullClassName();
        String indexKey = Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY;
        
        if (propObj instanceof Node)
        {
            if (!isSuperclass) db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Node)propObj, indexKey, className + "-EXCLUSIVE");
            db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Node)propObj, indexKey, className);
        }
        else
        {
            if (!isSuperclass) db.index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Relationship)propObj, indexKey, className + "-EXCLUSIVE");
            db.index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Relationship)propObj, indexKey, className);
        }

        if (cmd.getSuperAbstractClassMetaData() != null)
        {
            addPropertyContainerToTypeIndex(db, propObj, cmd.getSuperAbstractClassMetaData(), true);
        }
    }
}
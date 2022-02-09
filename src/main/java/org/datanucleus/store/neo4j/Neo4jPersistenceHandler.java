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
package org.datanucleus.store.neo4j;

import java.util.Iterator;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
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
 * Persistence Handler for Neo4j.
 */
public class Neo4jPersistenceHandler extends AbstractPersistenceHandler
{
    public Neo4jPersistenceHandler(StoreManager storeMgr)
    {
        super(storeMgr);
    }

    @Override
    public void close()
    {
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

            // Do initial insert to create PropertyContainers (Node/Relationship)
            for (DNStateManager sm : sms)
            {
                insertObjectToPropertyContainer(sm, db);
            }

            // Do second pass for relation fields
            for (DNStateManager sm : sms)
            {
                AbstractClassMetaData cmd = sm.getClassMetaData();
                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                if (sd == null)
                {
                    storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
                    sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                }
                Table table = sd.getTable();

                PropertyContainer propObj = (PropertyContainer)sm.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);

                // Process relation fields
                int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
                if (relPositions.length > 0)
                {
                    StoreFieldManager fm = new StoreFieldManager(sm, propObj, true, table);
                    sm.provideFields(relPositions, fm);
                }
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting objects ", e);
            throw new NucleusDataStoreException("Exception inserting objects", e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Method that checks for existence of a PropertyContainer for the specified StateManager,
     * and creates it when not existing, setting all properties except for any relation fields.
     * @param sm StateManager
     * @param db The GraphDB
     * @return The PropertyContainer
     * @throws NucleusUserException if a property container exists already with this identity or if an error
     *     occurred during persistence.
     */
    public PropertyContainer insertObjectToPropertyContainer(DNStateManager sm, GraphDatabaseService db)
    {
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        if ((cmd.getIdentityType() == IdentityType.APPLICATION || cmd.getIdentityType() == IdentityType.DATASTORE) &&
            !cmd.pkIsDatastoreAttributed(storeMgr))
        {
            // Enforce uniqueness of datastore rows
            try
            {
                locateObject(sm);
                throw new NucleusUserException(Localiser.msg("Neo4j.Insert.ObjectWithIdAlreadyExists", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }
            catch (NucleusObjectNotFoundException onfe)
            {
                // Do nothing since object with this id doesn't exist
            }
        }

        ExecutionContext ec = sm.getExecutionContext();
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
            sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        // Create the PropertyContainer; currently only support as a Node.
        // TODO Support persisting as "attributed relation" where the object has source and target objects and no other relation field
        PropertyContainer propObj = db.createNode();
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Persisting " + sm + " as " + propObj);
        }
        addPropertyContainerToTypeIndex(db, propObj, cmd, false);

        // Cache the PropertyContainer with StateManager
        sm.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);

        if (cmd.pkIsDatastoreAttributed(storeMgr))
        {
            long id = (propObj instanceof Node ? ((Node)propObj).getId() : ((Relationship)propObj).getId());

            // Set the identity of the object based on the datastore-generated IDENTITY strategy value
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                sm.setPostStoreNewObjectId(id);
                if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithIdentity", sm.getObjectAsPrintable(), id));
                }
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNumbers = cmd.getPKMemberPositions();
                for (int i=0;i<pkFieldNumbers.length;i++)
                {
                    AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkFieldNumbers[i]);
                    if (storeMgr.isValueGenerationStrategyDatastoreAttributed(cmd, pkFieldNumbers[i]))
                    {
                        if (!Number.class.isAssignableFrom(mmd.getType()) && mmd.getType() != long.class && mmd.getType() != int.class)
                        {
                            // Field type must be Long since Neo4j node id is a long
                            throw new NucleusUserException("Any field using IDENTITY value generation with Neo4j should be of type numeric");
                        }
                        sm.setPostStoreNewObjectId(id);
                        if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
                        {
                            NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithIdentity", sm.getObjectAsPrintable(), id));
                        }
                    }
                }
            }
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
                long versionNumber = 1;
                sm.setTransactionalVersion(Long.valueOf(versionNumber));
                if (NucleusLogger.DATASTORE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE.debug(Localiser.msg("Neo4j.Insert.ObjectPersistedWithVersion",
                        sm.getObjectAsPrintable(), sm.getInternalObjectId(), "" + versionNumber));
                }
                if (vermd.getMemberName() != null)
                {
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getMemberName());
                    Object verFieldValue = Long.valueOf(versionNumber);
                    if (verMmd.getType() == int.class || verMmd.getType() == Integer.class)
                    {
                        verFieldValue = Integer.valueOf((int)versionNumber);
                    }
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), verFieldValue);
                }
                else
                {
                    propObj.setProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName(), versionNumber);
                }
            }
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            // Discriminator field
            String propName = table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName();
            propObj.setProperty(propName, cmd.getDiscriminatorValue());
        }

        Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
        if (multitenancyCol != null)
        {
            String tenantId = ec.getTenantId();
            if (tenantId != null)
            {
                // Multi-tenancy discriminator
                propObj.setProperty(multitenancyCol.getName(), tenantId);
            }
        }

        Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
        if (softDeleteCol != null)
        {
            // Soft-delete flag
            propObj.setProperty(softDeleteCol.getName(), Boolean.FALSE);
        }

        // Insert non-relation fields
        int[] nonRelPositions = cmd.getNonRelationMemberPositions(ec.getClassLoaderResolver());
        StoreFieldManager fm = new StoreFieldManager(sm, propObj, true, table);
        sm.provideFields(nonRelPositions, fm);

        return propObj;
    }

    @Override
    public void insertObject(DNStateManager sm)
    {
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
 
            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Insert.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            // Step 1 : Create PropertyContainer with non-relation fields
            PropertyContainer propObj = insertObjectToPropertyContainer(sm, db);
            AbstractClassMetaData cmd = sm.getClassMetaData();
            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            // Step 2 : Set relation fields
            int[] relPositions = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
            if (relPositions.length > 0)
            {
                StoreFieldManager fm = new StoreFieldManager(sm, propObj, true, table);
                sm.provideFields(relPositions, fm);
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementInsertCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception inserting object " + sm, e);
            throw new NucleusDataStoreException("Exception inserting object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * When a Node is inserted it is added to the index "DN_TYPES" with the key "class" set to
     * {class-name}, {class-name}-EXCUSIVE, as well as any persistable superclasses.
     * This allows it to be considered in queries where the candidate is a supertype, or is exclusively
     * this type, etc.
     * @param db The GraphDatabaseService
     * @param propObj The Node/Relationship to add
     * @param cmd Meta-data for the class
     * @param superclass Whether this is processing a superclass of the real type
     */
    private void addPropertyContainerToTypeIndex(GraphDatabaseService db, PropertyContainer propObj, 
            AbstractClassMetaData cmd, boolean superclass)
    {
        if (propObj instanceof Node)
        {
            if (!superclass)
            {
                // Processing the real type, so add to "-EXCLUSIVE" also
                db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Node)propObj,
                    Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, cmd.getFullClassName() + "-EXCLUSIVE");
            }

            db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Node)propObj, 
                Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, cmd.getFullClassName());
        }
        else
        {
            if (!superclass)
            {
                // Processing the real type, so add to "-EXCLUSIVE" also
                db.index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Relationship)propObj,
                    Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, cmd.getFullClassName() + "-EXCLUSIVE");
            }

            db.index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).add((Relationship)propObj, 
                Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, cmd.getFullClassName());
        }

        AbstractClassMetaData superCmd = cmd.getSuperAbstractClassMetaData();
        if (superCmd != null)
        {
            // Recurse up to superclasses since an instance of cmd is also an instance of superCmd
            addPropertyContainerToTypeIndex(db, propObj, superCmd, true);
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
            GraphDatabaseService db = (GraphDatabaseService) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            AbstractClassMetaData cmd = sm.getClassMetaData();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                StringBuilder fieldStr = new StringBuilder();
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        fieldStr.append(",");
                    }
                    fieldStr.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Update.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId(), fieldStr.toString()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            PropertyContainer propObj = Neo4jUtils.getPropertyContainerForStateManager(db, sm);
            if (propObj == null)
            {
                if (cmd.isVersioned())
                {
                    throw new NucleusOptimisticException("Object with id " + sm.getInternalObjectId() + 
                        " and version " + sm.getTransactionalVersion() + " no longer present");
                }
                throw new NucleusDataStoreException("Could not find object with id " + sm.getInternalObjectId());
            }

            int[] updatedFieldNums = fieldNumbers;
            if (cmd.isVersioned())
            {
                // Version object so calculate version to store with
                Object currentVersion = sm.getTransactionalVersion();
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                Object nextVersion = ec.getLockManager().getNextVersion(vermd, currentVersion);
                sm.setTransactionalVersion(nextVersion);

                if (vermd.getMemberName() != null)
                {
                    // Update the version field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getMemberName());
                    sm.replaceField(verMmd.getAbsoluteFieldNumber(), nextVersion);

                    boolean updatingVerField = false;
                    for (int i=0;i<fieldNumbers.length;i++)
                    {
                        if (fieldNumbers[i] == verMmd.getAbsoluteFieldNumber())
                        {
                            updatingVerField = true;
                        }
                    }
                    if (!updatingVerField)
                    {
                        // Add the version field to the fields to be updated
                        updatedFieldNums = new int[fieldNumbers.length+1];
                        System.arraycopy(fieldNumbers, 0, updatedFieldNums, 0, fieldNumbers.length);
                        updatedFieldNums[fieldNumbers.length] = verMmd.getAbsoluteFieldNumber();
                    }
                }
                else
                {
                    // Update the stored surrogate value
                    propObj.setProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName(), nextVersion);
                }
            }

            StoreFieldManager fm = new StoreFieldManager(sm, propObj, false, table);
            sm.provideFields(updatedFieldNums, fm);

            if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Updating " + sm + " in " + propObj);
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementUpdateCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", 
                    (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception updating object " + sm, e);
            throw new NucleusDataStoreException("Exception updating object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void deleteObject(DNStateManager sm)
    {
        // Check if read-only so update not permitted
        assertReadOnlyForUpdateOfObject(sm);

        AbstractClassMetaData cmd = sm.getClassMetaData();
        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();

            PropertyContainer propObj = Neo4jUtils.getPropertyContainerForStateManager(db, sm);
            if (propObj == null)
            {
                throw new NucleusException("Attempt to delete " + sm + " yet no Node/Relationship found! See the log for details");
            }

            // Invoke any cascade deletion
            sm.loadUnloadedFields();
            int[] relMemberPosns = cmd.getRelationMemberPositions(ec.getClassLoaderResolver());
            sm.provideFields(relMemberPosns, new DeleteFieldManager(sm, true));

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.Delete.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            // TODO Support SOFT_DELETE
            if (propObj instanceof Node)
            {
                // Remove all Relationships for this Node
                Node node = (Node)propObj;
                Iterable<Relationship> rels = node.getRelationships();
                Iterator<Relationship> relIter = rels.iterator();
                while (relIter.hasNext())
                {
                    Relationship rel = relIter.next();
                    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_NATIVE.debug("Deleting relationship " + rel + " for " + node);
                    }
                    rel.delete();
                }

                // Remove it from the DN_TYPES index
                db.index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX).remove(node);

                // Delete this object
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Deleting " + sm + " as " + node);
                }
                node.delete();
            }
            else
            {
                // TODO Cater for persistence as Relationship
            }

            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementNumWrites();
                ec.getStatistics().incrementDeleteCount();
            }

            if (NucleusLogger.DATASTORE_PERSIST.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_PERSIST.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
        }
        catch (Exception e)
        {
            NucleusLogger.PERSISTENCE.error("Exception deleting object " + sm, e);
            throw new NucleusDataStoreException("Exception deleting object for " + sm, e);
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void fetchObject(DNStateManager sm, int[] fieldNumbers)
    {
        AbstractClassMetaData cmd = sm.getClassMetaData();

        ExecutionContext ec = sm.getExecutionContext();
        ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                // Debug information about what we are retrieving
                StringBuilder str = new StringBuilder("Fetching object \"");
                str.append(sm.getObjectAsPrintable()).append("\" (id=");
                str.append(sm.getInternalObjectId()).append(")").append(" fields [");
                for (int i=0;i<fieldNumbers.length;i++)
                {
                    if (i > 0)
                    {
                        str.append(",");
                    }
                    str.append(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumbers[i]).getName());
                }
                str.append("]");
                NucleusLogger.DATASTORE_RETRIEVE.debug(str.toString());
            }

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Neo4j.Fetch.Start", sm.getObjectAsPrintable(), sm.getInternalObjectId()));
            }

            StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            if (sd == null)
            {
                storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
                sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
            }
            Table table = sd.getTable();

            // Find the Node for this StateManager
            PropertyContainer propObj = Neo4jUtils.getPropertyContainerForStateManager(db, sm);
            if (propObj == null)
            {
                throw new NucleusObjectNotFoundException("Datastore object not found for id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()));
            }

            // Retrieve the fields required
            FieldManager fm = new FetchFieldManager(sm, propObj, table);
            sm.replaceFields(fieldNumbers, fm);

            if (cmd.isVersioned() && sm.getTransactionalVersion() == null)
            {
                // No version set, so retrieve it
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getMemberName() != null)
                {
                    // Version stored in a field
                    Object datastoreVersion = sm.provideField(cmd.getAbsolutePositionOfMember(vermd.getMemberName()));
                    sm.setVersion(datastoreVersion);
                }
                else
                {
                    // Surrogate version
                    Object datastoreVersion = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                    sm.setVersion(datastoreVersion);
                }
            }

            if (NucleusLogger.DATASTORE_RETRIEVE.isDebugEnabled())
            {
                NucleusLogger.DATASTORE_RETRIEVE.debug(Localiser.msg("Neo4j.ExecutionTime", (System.currentTimeMillis() - startTime)));
            }
            if (ec.getStatistics() != null)
            {
                ec.getStatistics().incrementFetchCount();
            }
        }
        catch (Exception e)
        {
            NucleusLogger.DATASTORE_RETRIEVE.error("Exception on fetch of fields", e);
            throw new NucleusDataStoreException("Exception on fetch of fields", e);
        }
        finally
        {
            mconn.release();
        }
    }

    @Override
    public void locateObject(DNStateManager sm)
    {
        final AbstractClassMetaData cmd = sm.getClassMetaData();
        if (cmd.getIdentityType() == IdentityType.APPLICATION || 
            cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            ExecutionContext ec = sm.getExecutionContext();
            ManagedConnection mconn = storeMgr.getConnectionManager().getConnection(ec);
            try
            {
                StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
                if (sd == null)
                {
                    storeMgr.manageClasses(sm.getExecutionContext().getClassLoaderResolver(), cmd.getFullClassName());
                }

                GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
                PropertyContainer propObj = Neo4jUtils.getPropertyContainerForStateManager(db, sm);
                if (propObj == null)
                {
                    throw new NucleusObjectNotFoundException("Object not found for id " + IdentityUtils.getPersistableIdentityForId(sm.getInternalObjectId()));
                }
            }
            finally
            {
                mconn.release();
            }
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StorePersistenceHandler#findObject(org.datanucleus.store.ExecutionContext, java.lang.Object)
     */
    public Object findObject(ExecutionContext ec, Object id)
    {
        return null;
    }
}
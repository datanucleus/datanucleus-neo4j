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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.FetchPlan;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.IdentityUtils;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.IdentityType;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.metadata.VersionMetaData;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.FieldValues;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.fieldmanager.FetchFieldManager;
import org.datanucleus.store.neo4j.query.LazyLoadQueryResult;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.TypeConversionHelper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

/**
 * Series of convenience methods to aid in the conversion between POJO and Neo4j Node.
 */
public class Neo4jUtils
{
    /**
     * Method to return the Node/Relationship representing the specified ObjectProvider.
     * @param graphDB Database service
     * @param op The object provider
     * @return The Node/Relationship for this ObjectProvider (or null if not found)
     * @throws NucleusException if more than 1 Node/Relationship is found matching this ObjectProvider!
     */
    public static PropertyContainer getPropertyContainerForObjectProvider(GraphDatabaseService graphDB, ObjectProvider op)
    {
        Object val = op.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER);
        if (val != null)
        {
            // Cached with ObjectProvider so return it
            return (PropertyContainer)val;
        }

        AbstractClassMetaData cmd = op.getClassMetaData();
        ExecutionContext ec = op.getExecutionContext();
        PropertyContainer propObj = getPropertyContainerForObjectId(graphDB, ec, cmd, op.getInternalObjectId());
        if (propObj != null)
        {
            // Cache the Node with the ObjectProvider
            op.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
        }
        return propObj;
    }

    /**
     * Method to return the Node/Relationship representing the object with the specified identity.
     * @param graphDB Database service
     * @param ec Execution Context
     * @param cmd Metadata for the root class that this could be an instance of
     * @param id The identity of the object
     * @return The Node/Relationship for this object (or null if not found)
     * @throws NucleusException if more than 1 Node/Relationship is found matching this object identity!
     */
    public static PropertyContainer getPropertyContainerForObjectId(GraphDatabaseService graphDB, ExecutionContext ec, AbstractClassMetaData cmd, Object id)
    {
        StoreManager storeMgr = ec.getStoreManager();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            storeMgr.manageClasses(clr, cmd.getFullClassName());
            sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();


        boolean attributedRelation = Neo4jUtils.classIsAttributedRelation(cmd);
        if (cmd.pkIsDatastoreAttributed(storeMgr))
        {
            // Using Neo4j "node id"/"relationship id" as the identity, so use getNodeById/getRelationshipById
            if (cmd.getIdentityType() == IdentityType.DATASTORE)
            {
                Long key = (Long)IdentityUtils.getTargetKeyForDatastoreIdentity(id);
                if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                {
                    NucleusLogger.DATASTORE_NATIVE.debug("Retrieving PropertyContainer for id=" + key);
                }
                return (attributedRelation ? graphDB.getRelationshipById(key) : graphDB.getNodeById(key));
            }
            else if (cmd.getIdentityType() == IdentityType.APPLICATION)
            {
                int[] pkFieldNumbers = cmd.getPKMemberPositions();
                if (pkFieldNumbers.length == 1)
                {
                    Long key = (Long)IdentityUtils.getTargetKeyForSingleFieldIdentity(id);
                    if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
                    {
                        NucleusLogger.DATASTORE_NATIVE.debug("Retrieving PropertyContainer for id=" + key);
                    }
                    return (attributedRelation ? graphDB.getRelationshipById(key) : graphDB.getNodeById(key));
                }

                // TODO Composite id using identity on one field?
            }
        }

        if (attributedRelation)
        {
            if (!graphDB.index().existsForRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX))
            {
                // No index yet so the Relationship can't exist
                return null;
            }
        }
        else
        {
            if (!graphDB.index().existsForNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX))
            {
                // No index yet so the Node can't exist
                return null;
            }
        }

        StringBuilder cypherString = new StringBuilder();

        // Start from the nodes/relationship of the required type
        cypherString.append("START pc=" + 
            (attributedRelation ? "relationship:" : "node:") + Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX + 
            "(" + Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY + "=\"" + cmd.getFullClassName() + "\")");

        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            // Application id - Add PK field(s) to the query object
            cypherString.append(" WHERE (");
            int[] pkPositions = cmd.getPKMemberPositions();
            for (int i=0;i<pkPositions.length;i++)
            {
                AbstractMemberMetaData pkMmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(pkPositions[i]);
                RelationType relType = pkMmd.getRelationType(clr);
                if (relType != RelationType.NONE)
                {
                    // TODO Support this. CompoundIdentity. Maybe we omit this and check after the retrieval for the right relation?
                    throw new NucleusException("We do not currently support retrieving objects with PK field that is a relation : " + pkMmd.getFullFieldName());
                }

                Object value = null;
                if (cmd.usesSingleFieldIdentityClass())
                {
                    value = IdentityUtils.getTargetKeyForSingleFieldIdentity(id);
                }
                else
                {
                    value = IdentityUtils.getValueForMemberInId(id, pkMmd);
                }

                cypherString.append("pc." + table.getMemberColumnMappingForMember(pkMmd).getColumn(0).getName());
                cypherString.append(" = ");
                Object storedValue = Neo4jUtils.getStoredValueForField(ec, pkMmd, value, FieldRole.ROLE_FIELD);
                if (storedValue instanceof String)
                {
                    cypherString.append("\"" + storedValue + "\"");
                }
                else
                {
                    cypherString.append(storedValue);
                }
                if (i != pkPositions.length-1)
                {
                    cypherString.append(" and ");
                }
            }
            cypherString.append(")");
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            if (id == null || storeMgr.isStrategyDatastoreAttributed(cmd, -1))
            {
                // Not yet set, so return null (needs to be attributed in the datastore)
                return null;
            }
            Object value = IdentityUtils.getTargetKeyForDatastoreIdentity(id);
            String propName = table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName();
            cypherString.append(" WHERE (pc.");
            cypherString.append(propName);
            cypherString.append(" = ");
            cypherString.append(Neo4jUtils.getStoredValueForField(ec, null, value, FieldRole.ROLE_FIELD));
            cypherString.append(")");
        }
        else
        {
            throw new NucleusException("Impossible to retrieve Node/Relationship for nondurable identity");
        }

        if (cmd.hasDiscriminatorStrategy())
        {
            String propName = table.getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName();
            Object discVal = cmd.getDiscriminatorValue();
            cypherString.append(" and (pc.").append(propName).append(" = \"").append(discVal).append("\")");
        }

        cypherString.append(" RETURN pc");

        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            NucleusLogger.DATASTORE_NATIVE.debug("Retrieving object using Cypher query : " + cypherString);
        }

        Result result = graphDB.execute(cypherString.toString());
        if (ec.getStatistics() != null)
        {
            // Add to statistics
            ec.getStatistics().incrementNumReads();
        }

        if (!result.hasNext())
        {
            result.close();
            return null;
        }

        Map<String, Object> map = result.next();
        PropertyContainer propObj = (PropertyContainer)map.get("pc");
        if (result.hasNext())
        {
            throw new NucleusException("Query of Node/Relationship for object with id=" + id + " returned more than 1 result! : " + cypherString);
        }
        result.close();
        return propObj;
    }

    /**
     * Method to execute a Cypher query.
     * @param query Query that is invoking this Cypher query
     * @param db The GraphDatabaseService to execute against
     * @param cypherText The Cypher query
     * @param candidateCmd Meta-data for the candidate of this query (if returning candidate)
     * @return List of results. If a result clause is provided then returns List&lt;Object&gt; or List&lt;Object[]&gt;
     *     and otherwise returns List&lt;candidate&gt;
     */
    public static List executeCypherQuery(Query query, GraphDatabaseService db, String cypherText, AbstractClassMetaData candidateCmd)
    {
        if (NucleusLogger.DATASTORE_NATIVE.isDebugEnabled())
        {
            if (candidateCmd != null)
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Retrieving objects of type " + candidateCmd.getFullClassName() + " using Cypher query : " + cypherText);
            }
            else
            {
                NucleusLogger.DATASTORE_NATIVE.debug("Retrieving objects using Cypher query : " + cypherText);
            }
        }

        // Extract the result from the Cypher text
        int resultStart = cypherText.indexOf("RETURN ") + 7;
        String resultStr = cypherText.substring(resultStart);
        int orderByStart = resultStr.indexOf(" ORDER BY");
        if (orderByStart > 0)
        {
            resultStr = resultStr.substring(0, orderByStart);
        }
        int skipStart = resultStr.indexOf(" SKIP");
        if (skipStart > 0)
        {
            resultStr = resultStr.substring(0, skipStart);
        }
        int limitStart = resultStr.indexOf(" LIMIT");
        if (limitStart > 0)
        {
            resultStr = resultStr.substring(0, limitStart);
        }
        if (resultStr.equals(query.getCompilation().getCandidateAlias()))
        {
            resultStr = null;
        }

        Result queryResult = db.execute(cypherText);

        // Return as lazy-load results object
        return new LazyLoadQueryResult(query, queryResult, resultStr);
    }

    /**
     * Convenience method to construct a cypher query string from candidate information as well as any required
     * filter, result, ordering and range.
     * If the ExecutionContext supports multi-tenancy then will also add a filter on the multitenancy property
     * @param ec ExecutionContext
     * @param cmd Metadata for the candidate
     * @param candidateAlias Alias for the candidate
     * @param subclasses Whether to include subclasses
     * @param filterText Any text to apply to the filter (optional)
     * @param resultText Any result text (optional)
     * @param orderText Any order text (optional)
     * @param rangeFromIncl Lower (inclusive) constraint on range
     * @param rangeToExcl Upper (exclusive) constraint on range
     * @return The Cypher string
     */
    public static String getCypherTextForQuery(ExecutionContext ec, AbstractClassMetaData cmd, String candidateAlias,
            boolean subclasses, String filterText, String resultText, String orderText, Long rangeFromIncl, Long rangeToExcl)
    {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        boolean attributedRelation = Neo4jUtils.classIsAttributedRelation(cmd);
        if (candidateAlias == null)
        {
            candidateAlias = (attributedRelation ? "r" : "n");
        }

        StringBuilder cypherString = new StringBuilder();

        // Start from the nodes of the required type
        String indexClassName = cmd.getFullClassName();
        if (!subclasses)
        {
            indexClassName += "-EXCLUSIVE";
        }
        cypherString.append("START " + candidateAlias + 
            (attributedRelation ? "=relationship:" : "=node:") + Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX + 
            "(" + Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY + "=\"" + indexClassName + "\")");

        // Add any WHERE clause
        boolean multiple = false;

        String multitenancyText = null;
        if (ec.getNucleusContext().isClassMultiTenant(cmd))
        {
            // Restriction on multitenancy discriminator for this tenant
            String propName = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY).getName();
            String value = ec.getNucleusContext().getMultiTenancyId(ec, cmd);
            multitenancyText = propName + " = \"" + value + "\"";
            if (filterText != null)
            {
                multiple = true;
            }
        }

        String softDeleteText = null;
        if (table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE) != null)
        {
            // Restriction on soft-delete flag
            String propName = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE).getName();
            softDeleteText = propName + " = \"" + Boolean.FALSE + "\"";
            if (filterText != null)
            {
                multiple = true;
            }
        }

        if (filterText != null || multitenancyText != null || softDeleteText != null)
        {
            cypherString.append(" WHERE ");
            boolean started = false;
            if (filterText != null)
            {
                if (multiple)
                {
                    cypherString.append("(");
                }
                cypherString.append(filterText);
                if (multiple)
                {
                    cypherString.append(")");
                }
                started = true;
            }
            if (multitenancyText != null)
            {
                if (started)
                {
                    cypherString.append(" and");
                }
                if (multiple)
                {
                    cypherString.append("(");
                }
                cypherString.append(multitenancyText);
                if (multiple)
                {
                    cypherString.append(")");
                }
            }
            if (softDeleteText != null)
            {
                if (started)
                {
                    cypherString.append(" and");
                }
                if (multiple)
                {
                    cypherString.append("(");
                }
                cypherString.append(softDeleteText);
                if (multiple)
                {
                    cypherString.append(")");
                }
            }
        }

        // Result
        if (resultText != null)
        {
            cypherString.append(" RETURN " + resultText);
        }
        else
        {
            cypherString.append(" RETURN " + candidateAlias);
        }

        // Ordering
        if (orderText != null)
        {
            cypherString.append(" ORDER BY " + orderText);
        }

        // Range
        if (rangeFromIncl != null || rangeToExcl != null)
        {
            long lower = 0;
            if (rangeFromIncl != null)
            {
                lower = rangeFromIncl;
                cypherString.append(" SKIP " + rangeFromIncl);
            }
            if (rangeToExcl != null)
            {
                cypherString.append(" LIMIT " + (rangeToExcl-lower));
            }
        }

        return cypherString.toString();
    }

    /**
     * Convenience method to return the inheritance level of a Node/Relationship, providing the root class that it
     * definitely is an instance of. Interrogates the node information for the types index to find the subclass
     * that this Node/Relationship belongs to.
     * @param propObj The Node/Relationship
     * @param ec Execution Context
     * @param cmd Root metadata
     * @return AbstractClassMetaData for the type that this Node/Relationship is an instance of.
     */
    public static AbstractClassMetaData getClassMetaDataForPropertyContainer(PropertyContainer propObj, ExecutionContext ec, AbstractClassMetaData cmd)
    {
        // Assumed to be of the root type, but check all subclasses
        boolean attributedRelation = Neo4jUtils.classIsAttributedRelation(cmd);
        if (attributedRelation)
        {
            Index<Relationship> typesIdx = propObj.getGraphDatabase().index().forRelationships(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX);

            String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(cmd.getFullClassName(), false);
            if (subclassNames != null)
            {
                for (int i=0;i<subclassNames.length;i++)
                {
                    AbstractClassMetaData subcmd = ec.getMetaDataManager().getMetaDataForClass(subclassNames[i], ec.getClassLoaderResolver());
                    IndexHits<Relationship> relsForSubclass = typesIdx.get(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, subcmd.getFullClassName());
                    if (relsForSubclass != null)
                    {
                        for (Relationship subclassRel : relsForSubclass)
                        {
                            if (subclassRel.equals(propObj))
                            {
                                return getClassMetaDataForPropertyContainer(subclassRel, ec, subcmd);
                            }
                        }
                    }
                }
            }
        }
        else
        {
            Index<Node> typesIdx = propObj.getGraphDatabase().index().forNodes(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX);

            String[] subclassNames = ec.getMetaDataManager().getSubclassesForClass(cmd.getFullClassName(), false);
            if (subclassNames != null)
            {
                for (int i=0;i<subclassNames.length;i++)
                {
                    AbstractClassMetaData subcmd = ec.getMetaDataManager().getMetaDataForClass(subclassNames[i], ec.getClassLoaderResolver());
                    IndexHits<Node> nodesForSubclass = typesIdx.get(Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY, subcmd.getFullClassName());
                    if (nodesForSubclass != null)
                    {
                        for (Node subclassNode : nodesForSubclass)
                        {
                            if (subclassNode.equals(propObj))
                            {
                                return getClassMetaDataForPropertyContainer(subclassNode, ec, subcmd);
                            }
                        }
                    }
                }
            }
        }

        return cmd;
    }

    /**
     * Convenience method to return the POJO that a Node equates to.
     * Checks the caches and, if not found, creates an object and populates the fetch plan values in, storing
     * the Node in the ObjectProvider "associatedValues" for future reference.
     * @param propObj The Node/Relationship
     * @param cmd Metadata for the class that this is an instance of (or subclass of)
     * @param ec ExecutionContext
     * @param ignoreCache Whether we should ignore the cache
     * @return The POJO
     */
    public static Object getObjectForPropertyContainer(PropertyContainer propObj, AbstractClassMetaData cmd, 
            ExecutionContext ec, boolean ignoreCache)
    {
        int[] fpMembers = ec.getFetchPlan().getFetchPlanForClass(cmd).getMemberNumbers();

        Object obj = null;
        if (cmd.getIdentityType() == IdentityType.APPLICATION)
        {
            obj = getObjectUsingApplicationIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        else if (cmd.getIdentityType() == IdentityType.DATASTORE)
        {
            obj = getObjectUsingDatastoreIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        else
        {
            obj = getObjectUsingNondurableIdForDBObject(propObj, cmd, ec, ignoreCache, fpMembers);
        }
        return obj;
    }

    protected static Object getObjectUsingApplicationIdForDBObject(final PropertyContainer propObj, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        final FieldManager fm = new FetchFieldManager(ec, propObj, cmd, table);
        Object id = IdentityUtils.getApplicationIdentityForResultSetRow(ec, cmd, null, false, fm);

        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);

        if (op.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created
            // so load the fieldValues from it.
            op.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    version = op.provideField(verMmd.getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                op.setVersion(version);
            }

            // Any fields loaded above will not be wrapped since we did not have the ObjectProvider at the point of creating the FetchFieldManager, so wrap them now
            op.replaceAllLoadedSCOFieldsWithWrappers();
        }

        return pc;
    }

    protected static Object getObjectUsingDatastoreIdForDBObject(final PropertyContainer propObj, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();
        Object idKey = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.DATASTORE_ID).getName());

        Object id = ec.getNucleusContext().getIdentityManager().getDatastoreId(cmd.getFullClassName(), idKey);
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);
        if (op.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created so load the fieldValues from it.
            op.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            final FieldManager fm = new FetchFieldManager(op, propObj, table);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    version = op.provideField(cmd.getMetaDataForMember(vermd.getFieldName()).getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                op.setVersion(version);
            }
        }

        return pc;
    }

    protected static Object getObjectUsingNondurableIdForDBObject(final PropertyContainer propObj, 
            final AbstractClassMetaData cmd, final ExecutionContext ec, boolean ignoreCache, final int[] fpMembers)
    {
        SCOID id = new SCOID(cmd.getFullClassName());
        Class type = ec.getClassLoaderResolver().classForName(cmd.getFullClassName());
        Object pc = ec.findObject(id, false, false, type.getName());
        ObjectProvider op = ec.findObjectProvider(pc);

        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        if (sd == null)
        {
            ec.getStoreManager().manageClasses(ec.getClassLoaderResolver(), cmd.getFullClassName());
            sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        }
        Table table = sd.getTable();

        if (op.getAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER) == null)
        {
            // The returned ObjectProvider doesn't have this Node/Relationship assigned to it hence must be just created
            // so load the fieldValues from it.
            op.setAssociatedValue(Neo4jStoreManager.OBJECT_PROVIDER_PROPCONTAINER, propObj);
            final FieldManager fm = new FetchFieldManager(op, propObj, table);
            op.loadFieldValues(new FieldValues()
            {
                public void fetchFields(ObjectProvider op)
                {
                    op.replaceFields(fpMembers, fm);
                }
                public void fetchNonLoadedFields(ObjectProvider op)
                {
                    op.replaceNonLoadedFields(fpMembers, fm);
                }
                public FetchPlan getFetchPlanForLoading()
                {
                    return null;
                }
            });

            if (cmd.isVersioned())
            {
                // Set the version on the retrieved object
                Object version = null;
                VersionMetaData vermd = cmd.getVersionMetaDataForClass();
                if (vermd.getFieldName() != null)
                {
                    // Get the version from the field value
                    AbstractMemberMetaData verMmd = cmd.getMetaDataForMember(vermd.getFieldName());
                    version = op.provideField(verMmd.getAbsoluteFieldNumber());
                }
                else
                {
                    // Get the surrogate version from the datastore
                    version = propObj.getProperty(table.getSurrogateColumn(SurrogateColumnType.VERSION).getName());
                }
                op.setVersion(version);
            }
        }
        return pc;
    }

    /**
     * Convenience method to return the value to store for the specified field and value taking into account
     * the types that are supported in Neo4j.
     * @param ec ExecutionContext
     * @param mmd Metadata for the member
     * @param value The value of the field
     * @param fieldRole The role of the field
     * @return The value to store
     */
    public static Object getStoredValueForField(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole)
    {
        if (value == null)
        {
            return null;
        }

        boolean optional = (mmd != null ? Optional.class.isAssignableFrom(mmd.getType()) : false);

        Class type = value.getClass();
        if (mmd != null)
        {
            if (optional)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else
            {
                if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                }
                else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
                }
                else if (fieldRole == FieldRole.ROLE_MAP_KEY)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                }
                else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                }
                else
                {
                    type = mmd.getType();
                }
            }
        }

        if (mmd != null && mmd.hasCollection() && !optional && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection rawColl = (Collection)value;
            if (rawColl.isEmpty())
            {
                return null;
            }

            Object[] objArray = new Object[rawColl.size()];
            int i = 0;
            for (Object elem : rawColl)
            {
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                objArray[i++] = storeElem;
            }

            // Convert to an accepted array type if necessary
            return convertArrayToStorableArray(objArray, mmd);
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            if (Array.getLength(value) == 0)
            {
                return null;
            }

            if (type.getComponentType().isPrimitive())
            {
                return value;
            }
            else if (type.getComponentType() == String.class)
            {
                return value;
            }

            Object[] objArray = new Object[Array.getLength(value)];
            for (int i=0;i<objArray.length;i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                objArray[i] = storeElem;
            }

            // Convert to an accepted array type if necessary
            return convertArrayToStorableArray(objArray, mmd);
        }

        if (Byte.class.isAssignableFrom(type) ||
            Boolean.class.isAssignableFrom(type) ||
            Character.class.isAssignableFrom(type) ||
            Double.class.isAssignableFrom(type) ||
            Float.class.isAssignableFrom(type) ||
            Integer.class.isAssignableFrom(type) ||
            Long.class.isAssignableFrom(type) ||
            Short.class.isAssignableFrom(type) ||
            String.class.isAssignableFrom(type))
        {
            // Natively supported
            return value;
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            return TypeConversionHelper.getStoredValueFromEnum(mmd, fieldRole, (Enum) value);
        }

        // Fallback to built-in type converters
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // store as a String
            return strConv.toDatastoreType(value);
        }
        else if (longConv != null)
        {
            // store as a Long
            return longConv.toDatastoreType(value);
        }

        // TODO Cater for cases with no converters
        return value;
    }

    private static Object convertArrayToStorableArray(Object[] objArray, AbstractMemberMetaData mmd)
    {
        if (objArray == null || objArray.length == 0)
        {
            return null;
        }

        // Convert to an accepted array type if necessary
        Object array = objArray;
        Class cmptCls = objArray[0].getClass();
        if (ClassUtils.isPrimitiveWrapperType(cmptCls.getName()))
        {
            // Primitive wrapper so convert to the primitive array type (ignores null elements)
            Class primType = ClassUtils.getPrimitiveTypeForType(cmptCls);
            array = Array.newInstance(primType, objArray.length);
            for (int i=0;i<objArray.length;i++)
            {
                Array.set(array, i, objArray[i]);
            }
        }
        else if (cmptCls.isPrimitive() || cmptCls == String.class)
        {
            array = Array.newInstance(cmptCls, objArray.length);
            for (int i=0;i<objArray.length;i++)
            {
                Array.set(array, i, objArray[i]);
            }
        }
        else
        {
            throw new NucleusException("Field " + mmd.getFullFieldName() + 
                " cannot be persisted to Neo4j since Neo4j doesn't natively support such a type (" + mmd.getType() + ")");
        }

        return array;
    }

    /**
     * Convenience method to convert the stored value for an object field into the value that will be held
     * in the object. Note that this does not cater for relation fields, just basic fields.
     * @param ec ExecutionContext
     * @param mmd Metadata for the field holding this value
     * @param value The stored value for the field
     * @param fieldRole The role of this value for the field
     * @return The value to put in the field
     */
    public static Object getFieldValueFromStored(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole)
    {
        if (value == null)
        {
            return null;
        }

        boolean optional = (mmd != null ? Optional.class.isAssignableFrom(mmd.getType()) : false);

        Class type = value.getClass();
        if (mmd != null)
        {
            if (optional)
            {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            }
            else
            {
                if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                }
                else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
                }
                else if (fieldRole == FieldRole.ROLE_MAP_KEY)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                }
                else if (fieldRole == FieldRole.ROLE_MAP_VALUE)
                {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                }
                else
                {
                    type = mmd.getType();
                }
            }
        }

        if (mmd != null && mmd.hasCollection() && !optional && fieldRole == FieldRole.ROLE_FIELD)
        {
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            for (int i=0;i<Array.getLength(value);i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
                coll.add(storeElem);
            }
            return coll;
        }
        else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD)
        {
            Object array = Array.newInstance(mmd.getType().getComponentType(), Array.getLength(value));
            for (int i=0;i<Array.getLength(value);i++)
            {
                Object elem = Array.get(value, i);
                Object storeElem = getFieldValueFromStored(ec, mmd, elem, FieldRole.ROLE_ARRAY_ELEMENT);
                Array.set(array, i++, storeElem);
            }
            return array;
        }

        if (Byte.class.isAssignableFrom(type) ||
            Boolean.class.isAssignableFrom(type) ||
            Character.class.isAssignableFrom(type) ||
            Double.class.isAssignableFrom(type) ||
            Float.class.isAssignableFrom(type) ||
            Integer.class.isAssignableFrom(type) ||
            Long.class.isAssignableFrom(type) ||
            Short.class.isAssignableFrom(type) ||
            String.class.isAssignableFrom(type))
        {
            return value;
        }
        else if (Enum.class.isAssignableFrom(type))
        {
            return TypeConversionHelper.getEnumForStoredValue(mmd, fieldRole, value, ec.getClassLoaderResolver());
        }

        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (strConv != null)
        {
            // Persisted as a String, so convert back
            String strValue = (String)value;
            return strConv.toMemberType(strValue);
        }
        else if (longConv != null)
        {
            // Persisted as a Long, so convert back
            Long longValue = (Long)value;
            return longConv.toMemberType(longValue);
        }

        // TODO Cater for cases with no converters
        return value;
    }

    /**
     * Convenience method to return the property (column) name for an embedded field.
     * @param ownerMmd Metadata for the field holding the embedded persistable object
     * @param fieldNumber FieldNumber of the embedded persistable object
     * @return Property name to use
     */
    public static String getPropertyNameForEmbeddedField(AbstractMemberMetaData ownerMmd, int fieldNumber)
    {
        String columnName = null;

        EmbeddedMetaData embmd = ownerMmd.getEmbeddedMetaData();
        AbstractMemberMetaData embMmd = null;
        if (embmd != null)
        {
            AbstractMemberMetaData[] embmmds = embmd.getMemberMetaData();
            embMmd = embmmds[fieldNumber];
        }

        if (embMmd != null)
        {
            ColumnMetaData[] colmds = embMmd.getColumnMetaData();
            if (colmds != null && colmds.length > 0)
            {
                // Try first column if specified
                columnName = colmds[0].getName();
            }
            if (columnName == null)
            {
                // Fallback to the field/property name
                columnName = embMmd.getName();
            }
            if (columnName == null)
            {
                columnName = embMmd.getName();
            }
        }
        return columnName;
    }

    /**
     * Convenience method that returns the actual class that this identity is an instance of.
     * Makes use of the DN_TYPES index to find the ultimate subclass that it is an instanceof.
     * @param id The identity
     * @param rootCmd ClassMetaData for the root class in the inheritance tree
     * @param ec ExecutionContext
     * @param clr ClassLoader resolver
     * @return The class name of the object with this id
     */
    public static String getClassNameForIdentity(Object id, AbstractClassMetaData rootCmd, ExecutionContext ec,
            ClassLoaderResolver clr)
    {
        StoreManager storeMgr = ec.getStoreManager();
        ManagedConnection mconn = storeMgr.getConnection(ec);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();
            PropertyContainer propObj = Neo4jUtils.getPropertyContainerForObjectId(db, ec, rootCmd, id);
            if (propObj instanceof Node)
            {
                return getClassMetaDataForPropertyContainer(propObj, ec, rootCmd).getFullClassName();
            }

            // Don't support polymorphism in objects persisted as Relationship TODO Allow this?
            return rootCmd.getFullClassName();
        }
        finally
        {
            mconn.release();
        }
    }

    /**
     * Return whether a class should be persisted as a Relationship rather than Node.
     * If a class is marked as an "attributed relation" then it needs to have 2 PC references, one for source
     * and one for target with both of those stored as Nodes.
     * @param cmd Metadata for the class to check
     * @return Whether it is to be stored as a Relationship
     */
    public static boolean classIsAttributedRelation(AbstractClassMetaData cmd)
    {
        if (cmd.hasExtension(Neo4jStoreManager.METADATA_ATTRIBUTED_RELATION))
        {
            if (cmd.getValueForExtension(Neo4jStoreManager.METADATA_ATTRIBUTED_RELATION).equalsIgnoreCase("TRUE"))
            {
                return true;
            }
        }
        return false;
    }
}
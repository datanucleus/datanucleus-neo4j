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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.ClassMetaData;
import org.datanucleus.metadata.ClassPersistenceModifier;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neo4j.query.JDOQLQuery;
import org.datanucleus.store.neo4j.query.JPQLQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.schema.table.CompleteClassTable;
import org.datanucleus.util.Localiser;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * StoreManager for persisting to Neo4j.
 */
public class Neo4jStoreManager extends AbstractStoreManager
{
    static
    {
        Localiser.registerBundle("org.datanucleus.store.neo4j.Localisation", Neo4jStoreManager.class.getClassLoader());
    }

    /** Key used for storing the PropertyContainer in an ObjectProvider associatedValues. */
    public static String OBJECT_PROVIDER_PROPCONTAINER = "DN_OP_PROPCONTAINER";

    public static String PROPCONTAINER_TYPE_INDEX = "DN_TYPES";
    public static String PROPCONTAINER_TYPE_INDEX_KEY = "class";

    public static String RELATIONSHIP_FIELD_NAME = "DN_FIELD_NAME";
    public static String RELATIONSHIP_FIELD_NAME_NONOWNER = "DN_FIELD_NAME_NONOWNER";
    public static String RELATIONSHIP_INDEX_NAME = "DN_CONTAINER_INDEX";

    /** Property name added to relationship to store the key of a map when we have Map&lt;NonPC, PC&gt; and the relationship is owner-value. */
    public static String RELATIONSHIP_MAP_KEY_VALUE = "DN_MAP_KEY";
    /** Property name added to relationship to store the value of a map when we have Map&lt;PC, NonPC&gt; and the relationship is owner-key. */
    public static String RELATIONSHIP_MAP_VAL_VALUE = "DN_MAP_VAL";

    /** key used in metadata for whether a class is persisted as an attributed relation (Relationship) */
    public static String METADATA_ATTRIBUTED_RELATION = "attributed-relation";

    /**
     * Constructor for a Neo4j StoreManager.
     * @param clr ClassLoader resolver
     * @param nucleusContext Nucleus Context
     * @param props Properties managed by this store
     */
    public Neo4jStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusContext, Map<String, Object> props)
    {
        super("neo4j", clr, nucleusContext, props);

        // Handler for persistence process
        persistenceHandler = new Neo4jPersistenceHandler(this);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add(StoreManager.OPTION_APPLICATION_ID);
        set.add(StoreManager.OPTION_APPLICATION_COMPOSITE_ID);
        set.add(StoreManager.OPTION_DATASTORE_ID);
        set.add(StoreManager.OPTION_NONDURABLE_ID);
        set.add(StoreManager.OPTION_ORM);
        set.add(StoreManager.OPTION_ORM_EMBEDDED_PC);
        set.add(StoreManager.OPTION_TXN_ISOLATION_READ_COMMITTED);
        set.add(StoreManager.OPTION_QUERY_JDOQL_BULK_DELETE);
        set.add(StoreManager.OPTION_QUERY_JPQL_BULK_DELETE);
        return set;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, java.lang.String)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec, queryString);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.StoreManager#newQuery(java.lang.String, org.datanucleus.ExecutionContext, org.datanucleus.store.query.Query)
     */
    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q)
    {
        if (language.equalsIgnoreCase(Query.LANGUAGE_JDOQL))
        {
            return new JDOQLQuery(this, ec, (JDOQLQuery) q);
        }
        else if (language.equalsIgnoreCase(Query.LANGUAGE_JPQL))
        {
            return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#getClassNameForObjectID(java.lang.Object, org.datanucleus.ClassLoaderResolver, org.datanucleus.store.ExecutionContext)
     */
    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec)
    {
        if (id == null)
        {
            // User stupidity
            return null;
        }
        else if (id instanceof SCOID)
        {
            // Object is a SCOID
            return ((SCOID) id).getSCOClass();
        }

        String rootClassName = super.getClassNameForObjectID(id, clr, ec);
        // TODO Allow for use of users-own PK class in multiple inheritance trees
        String[] subclasses = getMetaDataManager().getSubclassesForClass(rootClassName, true);
        if (subclasses == null || subclasses.length == 0)
        {
            // No subclasses so no need to go to the datastore
            return rootClassName;
        }

        AbstractClassMetaData rootCmd = getMetaDataManager().getMetaDataForClass(rootClassName, clr);
        return Neo4jUtils.getClassNameForIdentity(id, rootCmd, ec, clr);
    }

    /**
     * Accessor for whether this value strategy is supported.
     * Overrides the superclass to allow for "IDENTITY" since we support it and no entry in plugins for it.
     * @param strategy The strategy
     * @return Whether it is supported.
     */
    public boolean supportsValueGenerationStrategy(String strategy)
    {
        if (super.supportsValueGenerationStrategy(strategy))
        {
            return true;
        }

        // "identity" doesn't have an explicit entry in plugin since uses datastore capabilities
        if (strategy.equalsIgnoreCase("IDENTITY"))
        {
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.AbstractStoreManager#manageClasses(org.datanucleus.ClassLoaderResolver, java.lang.String[])
     */
    @Override
    public void manageClasses(ClassLoaderResolver clr, String... classNames)
    {
        if (classNames == null)
        {
            return;
        }

        ManagedConnection mconn = connectionMgr.getConnection(-1);
        try
        {
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();

            manageClasses(classNames, clr, db);
        }
        finally
        {
            mconn.release();
        }
    }

    public void manageClasses(String[] classNames, ClassLoaderResolver clr, GraphDatabaseService db)
    {
        if (classNames == null)
        {
            return;
        }

        // Filter out any "simple" type classes
        String[] filteredClassNames = getNucleusContext().getTypeManager().filterOutSupportedSecondClassNames(classNames);

        // Find the ClassMetaData for these classes and all referenced by these classes
        Set<String> clsNameSet = new HashSet<String>();
        Iterator iter = getMetaDataManager().getReferencedClasses(filteredClassNames, clr).iterator();
        while (iter.hasNext())
        {
            ClassMetaData cmd = (ClassMetaData)iter.next();
            if (cmd.getPersistenceModifier() == ClassPersistenceModifier.PERSISTENCE_CAPABLE && !cmd.isAbstract() && !cmd.isEmbeddedOnly())
            {
                if (!storeDataMgr.managesClass(cmd.getFullClassName()))
                {
                    StoreData sd = storeDataMgr.get(cmd.getFullClassName());
                    if (sd == null)
                    {
                        CompleteClassTable table = new CompleteClassTable(this, cmd, null);
                        sd = newStoreData(cmd, clr);
                        sd.setTable(table);
                        registerStoreData(sd);
                    }

                    clsNameSet.add(cmd.getFullClassName());
                }
            }
        }
    }
}
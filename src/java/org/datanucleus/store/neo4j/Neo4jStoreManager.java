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
import java.util.Map;
import java.util.Set;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.NucleusContext;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.AbstractStoreManager;

/**
 * StoreManager for persisting to Neo4j.
 */
public class Neo4jStoreManager extends AbstractStoreManager
{
    /** Key used for storing the PropertyContainer in an ObjectProvider associatedValues. */
    public static String OBJECT_PROVIDER_PROPCONTAINER = "DN_OP_PROPCONTAINER";

    public static String PROPCONTAINER_TYPE_INDEX = "DN_TYPES";
    public static String PROPCONTAINER_TYPE_INDEX_KEY = "class";

    public static String RELATIONSHIP_FIELD_NAME = "DN_FIELD_NAME";
    public static String RELATIONSHIP_FIELD_NAME_NONOWNER = "DN_FIELD_NAME_NONOWNER";
    public static String RELATIONSHIP_INDEX_NAME = "DN_CONTAINER_INDEX";

    /** Property name added to relationship to store the key of a map when we have Map<NonPC, PC> and the relationship is owner-value. */
    public static String RELATIONSHIP_MAP_KEY_VALUE = "DN_MAP_KEY";
    /** Property name added to relationship to store the value of a map when we have Map<PC, NonPC> and the relationship is owner-key. */
    public static String RELATIONSHIP_MAP_VAL_VALUE = "DN_MAP_VAL";

    /** key used in metadata for whether a class is persisted as an attributed relation (Relationship) */
    public static String METADATA_ATTRIBUTED_RELATION = "attributed-relation";

    /**
     * Constructor for a Neo4j StoreManager.
     * @param clr ClassLoader resolver
     * @param nucleusContext Nucleus Context
     * @param props Properties managed by this store
     */
    public Neo4jStoreManager(ClassLoaderResolver clr, NucleusContext nucleusContext, 
            Map<String, Object> props)
    {
        super("neo4j", clr, nucleusContext, props);

        // Handler for persistence process
        persistenceHandler = new Neo4jPersistenceHandler(this);

        logConfiguration();
    }

    public Collection getSupportedOptions()
    {
        Set set = new HashSet();
        set.add("ApplicationIdentity");
        set.add("DatastoreIdentity");
        set.add("NonDurableIdentity");
        set.add("ORM");
        set.add("TransactionIsolationLevel.read-committed");
        return set;
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
    public boolean supportsValueStrategy(String strategy)
    {
        if (super.supportsValueStrategy(strategy))
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
}
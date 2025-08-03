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
**********************************************************************/
package org.datanucleus.store.neo4j;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistenceNucleusContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.flush.FlushProcess;
import org.datanucleus.identity.SCOID;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.QueryLanguage;
import org.datanucleus.store.AbstractStoreManager;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.StorePersistenceHandler;
import org.datanucleus.store.connection.ConnectionFactory;
import org.datanucleus.store.connection.ConnectionManagerImpl;
import org.datanucleus.store.neo4j.query.BoltCypherQuery;
import org.datanucleus.store.neo4j.query.BoltJDOQLQuery;
import org.datanucleus.store.neo4j.query.CypherQuery;
import org.datanucleus.store.neo4j.query.JDOQLQuery;
import org.datanucleus.store.neo4j.query.JPQLQuery;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;

public class Neo4jStoreManager extends AbstractStoreManager {
    static {
        Localiser.registerBundle("org.datanucleus.store.neo4j.Localisation", Neo4jStoreManager.class.getClassLoader());
    }

    public static String OBJECT_PROVIDER_PROPCONTAINER = "DN_OP_PROPCONTAINER";
    public static String PROPCONTAINER_TYPE_INDEX = "DN_TYPES";
    public static String PROPCONTAINER_TYPE_INDEX_KEY = "class";
    public static String RELATIONSHIP_FIELD_NAME = "DN_FIELD_NAME";
    public static String RELATIONSHIP_FIELD_NAME_NONOWNER = "DN_FIELD_NAME_NONOWNER";
    public static String RELATIONSHIP_INDEX_NAME = "DN_CONTAINER_INDEX";
    public static String RELATIONSHIP_MAP_KEY_VALUE = "DN_MAP_KEY";
    public static String RELATIONSHIP_MAP_VAL_VALUE = "DN_MAP_VAL";
    public static String METADATA_ATTRIBUTED_RELATION = "attributed-relation";

    private boolean isRemoteConnection = false;

    public Neo4jStoreManager(ClassLoaderResolver clr, PersistenceNucleusContext nucleusContext, Map<String, Object> props) {
        super("neo4j", clr, nucleusContext, props);
        String connectionUrl = getStringProperty(PropertyNames.PROPERTY_CONNECTION_URL);

        if (connectionUrl != null && connectionUrl.toLowerCase().contains("bolt")) {
            this.isRemoteConnection = true;
            persistenceHandler = new BoltPersistenceHandler(this);
        } else {
            this.isRemoteConnection = false;
            try {
                // === THE FIX IS HERE: Use reflection to lazy-load the embedded handler ===
                // This prevents NoClassDefFoundError if the embedded driver JAR is not on the classpath.
                Class<?> handlerClass = clr.classForName("org.datanucleus.store.neo4j.embedded.Neo4jPersistenceHandler");
                Constructor<?> handlerConstructor = ClassUtils.getConstructorWithArguments(handlerClass, new Class[] { StoreManager.class });
                persistenceHandler = (StorePersistenceHandler) handlerConstructor.newInstance(this);
            } catch (Exception e) {
                NucleusLogger.DATASTORE.error("Failed to initialize Neo4jPersistenceHandler for embedded mode.", e);
                throw new NucleusException("Failed to initialize embedded Neo4jPersistenceHandler. Is the datanucleus-neo4j-embedded.jar on the classpath?", e);
            }
        }
        logConfiguration();
    }

    public boolean isRemoteConnection() {
        return isRemoteConnection;
    }

    @Override
    protected void registerConnectionMgr() {
        this.connectionMgr = new SmartConnectionManager(this);
    }

    private class SmartConnectionManager extends ConnectionManagerImpl {
        public SmartConnectionManager(StoreManager storeMgr) { super(storeMgr); }
        public ConnectionFactory newConnectionFactory(StoreManager storeMgr, String resourceName) {
            try {
                ClassLoaderResolver clr = nucleusContext.getClassLoaderResolver(null);
                String factoryClassName = isRemoteConnection ?
                    "org.datanucleus.store.neo4j.BoltConnectionFactoryImpl" :
                    "org.datanucleus.store.neo4j.ConnectionFactoryImpl";
                Class<?> factoryClass = clr.classForName(factoryClassName);
                Constructor<?> ctr = ClassUtils.getConstructorWithArguments(factoryClass, new Class[]{StoreManager.class, String.class});
                return (ConnectionFactory) ctr.newInstance(storeMgr, resourceName);
            } catch (Exception e) {
                throw new NucleusException("Error creating ConnectionFactory", e);
            }
        }
    }

    @Override
    public Collection<String> getSupportedQueryLanguages() {
        Collection<String> languages = super.getSupportedQueryLanguages();
        languages.add("Cypher");
        return languages;
    }

    @Override
    public boolean supportsQueryLanguage(String language) {
        if (language.equalsIgnoreCase("Cypher")) {
            return true;
        }
        return super.supportsQueryLanguage(language);
    }
    
    @Override
    public Query newQuery(String language, ExecutionContext ec) {
        if (language.equalsIgnoreCase("CYPHER")) {
            if (isRemoteConnection) { return new BoltCypherQuery(this, ec, (String) null); }
            return new CypherQuery(this, ec);
        }
        if (isRemoteConnection) { return new BoltJDOQLQuery(this, ec); }
        else {
            if (language.equals(QueryLanguage.JDOQL.name())) return new JDOQLQuery(this, ec);
            if (language.equals(QueryLanguage.JPQL.name())) return new JPQLQuery(this, ec);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    @Override
    public Query newQuery(String language, ExecutionContext ec, String queryString) {
        if (language.equalsIgnoreCase("CYPHER")) {
            if (isRemoteConnection) { return new BoltCypherQuery(this, ec, queryString); }
            return new CypherQuery(this, ec, queryString);
        }
        if (isRemoteConnection) { return new BoltJDOQLQuery(this, ec, queryString); }
        else {
            if (language.equals(QueryLanguage.JDOQL.name())) return new JDOQLQuery(this, ec, queryString);
            if (language.equals(QueryLanguage.JPQL.name())) return new JPQLQuery(this, ec, queryString);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    @Override
    public Query newQuery(String language, ExecutionContext ec, Query q) {
        if (language.equalsIgnoreCase("CYPHER")) {
            String queryString = (q instanceof AbstractJavaQuery) ? ((AbstractJavaQuery)q).getSingleStringQuery() : null;
            if (isRemoteConnection) { return new BoltCypherQuery(this, ec, queryString); }
            return new CypherQuery(this, ec, queryString);
        }
        if (isRemoteConnection) { return new BoltJDOQLQuery(this, ec, q); }
        else {
            if (language.equals(QueryLanguage.JDOQL.name())) return new JDOQLQuery(this, ec, (JDOQLQuery) q);
            if (language.equals(QueryLanguage.JPQL.name())) return new JPQLQuery(this, ec, (JPQLQuery) q);
        }
        throw new NucleusException("Error creating query for language " + language);
    }

    @SuppressWarnings("rawtypes")
    public Collection getSupportedOptions() {
        Set<String> set = new HashSet<>();
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

    @Override
    public String getClassNameForObjectID(Object id, ClassLoaderResolver clr, ExecutionContext ec) {
        if (id == null) return null;
        if (id instanceof SCOID) return ((SCOID) id).getSCOClass();
        return super.getClassNameForObjectID(id, clr, ec);
    }

    
    public boolean supportsValueGenerationStrategy(String strategy) {
        if (super.supportsValueGenerationStrategy(strategy)) return true;
        if (strategy.equalsIgnoreCase("IDENTITY")) return true;
        return false;
    }
}
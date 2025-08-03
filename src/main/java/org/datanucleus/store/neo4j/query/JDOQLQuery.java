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
***********************************************************************/
package org.datanucleus.store.neo4j.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.neo4j.EmbeddedQueryEngine;
import org.datanucleus.store.neo4j.Neo4jSchemaUtils;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.QueryManager;
import org.datanucleus.store.query.QueryResult;
import org.datanucleus.store.query.inmemory.JDOQLInMemoryEvaluator;
import org.datanucleus.store.query.inmemory.JavaQueryInMemoryEvaluator;
import org.datanucleus.util.Localiser;
import org.neo4j.graphdb.GraphDatabaseService;

public class JDOQLQuery extends AbstractJDOQLQuery {
    private static final long serialVersionUID = -360869109260191024L;
    protected transient Neo4jQueryCompilation datastoreCompilation = null;

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec) {
        this(storeMgr, ec, (JDOQLQuery) null);
    }

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, JDOQLQuery q) {
        super(storeMgr, ec, q);
    }

    public JDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec, query);
    }

    @Override
    protected void discardCompiled() {
        super.discardCompiled();
        datastoreCompilation = null;
    }

    @Override
    protected boolean isCompiled() {
        if (evaluateInMemory()) { return compilation != null; }
        if (compilation == null || datastoreCompilation == null) { return false; }
        if (!datastoreCompilation.isPrecompilable()) {
            datastoreCompilation = null;
            return false;
        }
        return true;
    }

    @Override
    protected synchronized void compileInternal(Map parameterValues) {
        System.out.println("DEBUG (JDOQLQuery): compileInternal called. Query: " + getSingleStringQuery());
        if (isCompiled()) {
            System.out.println("DEBUG (JDOQLQuery): Already compiled.");
            return;
        }
        super.compileInternal(parameterValues);
        if (candidateCollection != null && evaluateInMemory()) {
            System.out.println("DEBUG (JDOQLQuery): In-memory evaluation from candidate collection. No datastore compilation needed.");
            return;
        }
        if (candidateClass == null) {
            throw new NucleusUserException(Localiser.msg("021009", candidateClassName));
        }
        
        System.out.println("DEBUG (JDOQLQuery): Starting datastore compilation...");
        datastoreCompilation = new Neo4jQueryCompilation();
        if (!evaluateInMemory()) {
            compileQueryFull(parameterValues);
            System.out.println("DEBUG (JDOQLQuery): Datastore compilation complete. Cypher: " + datastoreCompilation.getCypherText());
        } else {
            System.out.println("DEBUG (JDOQLQuery): In-memory evaluation. Datastore compilation skipped.");
        }
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected Object performExecute(Map parameters) {
        System.out.println("DEBUG (JDOQLQuery): performExecute called. Query Type: " + type);
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try {
            GraphDatabaseService db = (GraphDatabaseService) mconn.getConnection();
            List<?> candidates;

            if (candidateCollection != null) {
                System.out.println("DEBUG (JDOQLQuery): Executing against provided candidate collection of size: " + candidateCollection.size());
                candidates = new ArrayList<>(candidateCollection);
            } else if (evaluateInMemory()) {
                System.out.println("DEBUG (JDOQLQuery): In-memory evaluation. Fetching all candidates of type " + candidateClass.getName());
                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(candidateClass, ec.getClassLoaderResolver());
                String cypherText = Neo4jSchemaUtils.getCypherTextForQuery(ec, cmd, compilation.getCandidateAlias(), subclasses, null, null, null, null, null);
                candidates = EmbeddedQueryEngine.executeCypherQuery(this, db, cypherText, Collections.emptyMap());
            } else {
                String cypherText = datastoreCompilation.getCypherText();
                System.out.println("DEBUG (JDOQLQuery): Executing datastore query with Cypher: " + cypherText);
                candidates = EmbeddedQueryEngine.executeCypherQuery(this, db, cypherText, parameters);
            }

            System.out.println("DEBUG (JDOQLQuery): Initial candidate result set size: " + candidates.size());
            Collection<?> results = candidates;
            if (evaluateInMemory() || !datastoreCompilation.isFilterComplete()) {
                System.out.println("DEBUG (JDOQLQuery): Applying in-memory filter/result mapping.");
                if (results instanceof QueryResult) { ((QueryResult) results).disconnect(); }
                JavaQueryInMemoryEvaluator resultMapper = new JDOQLInMemoryEvaluator(this, (Collection) results, compilation, parameters, ec.getClassLoaderResolver());
                results = resultMapper.execute(true, true, true, true, true);
                System.out.println("DEBUG (JDOQLQuery): Final in-memory result set size: " + results.size());
            }

            if (type == QueryType.BULK_DELETE) {
                System.out.println("DEBUG (JDOQLQuery): Query is BULK_DELETE. Deleting " + results.size() + " objects.");
                if (results instanceof QueryResult) { ((QueryResult) results).disconnect(); }
                ec.deleteObjects(results.toArray());
                System.out.println("DEBUG (JDOQLQuery): BULK_DELETE completed.");
                return (long) results.size();
            } else if (type == QueryType.BULK_UPDATE) {
                throw new NucleusException("Bulk Update is not supported for Neo4j JDOQL queries.");
            }
            
            return results;
        } finally {
            mconn.release();
        }
    }

    private void compileQueryFull(Map parameters) {
        AbstractClassMetaData cmd = getCandidateClassMetaData();
        QueryToCypherMapper mapper = new QueryToCypherMapper(compilation, parameters, cmd, ec, this);
        mapper.compile(datastoreCompilation);
    }

    @Override
    public Object getNativeQuery() {
        if (datastoreCompilation != null) {
            return datastoreCompilation.getCypherText();
        }
        return null;
    }
}
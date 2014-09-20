/**********************************************************************
Copyright (c) 2014 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/
package org.datanucleus.store.neo4j.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neo4j.Neo4jUtils;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Cypher query for Neo4j. Allows the user to execute a Cypher query and return the results in the form "List&lt;Object[]&gt;".
 */
public class CypherQuery extends AbstractJavaQuery
{
    private static final long serialVersionUID = 2808968696540162104L;

    String cypher;

    /**
     * Constructs a new query instance that uses the given execution context.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     */
    public CypherQuery(StoreManager storeMgr, ExecutionContext ec)
    {
        this(storeMgr, ec, (CypherQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param q The query from which to copy criteria.
     */
    public CypherQuery(StoreManager storeMgr, ExecutionContext ec, CypherQuery q)
    {
        super(storeMgr, ec);
        this.cypher = q.cypher;
    }

    /**
     * Constructor for a JDOQL query where the query is specified using the "Single-String" format.
     * @param storeMgr StoreManager for this query
     * @param ec execution context
     * @param query The query string
     */
    public CypherQuery(StoreManager storeMgr, ExecutionContext ec, String query)
    {
        super(storeMgr, ec);
        this.cypher = query;
    }

    /*
     * (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractJavaQuery#getSingleStringQuery()
     */
    @Override
    public String getSingleStringQuery()
    {
        return cypher;
    }

    public void compileGeneric(Map parameterValues)
    {
    }

    protected void compileInternal(Map parameterValues)
    {
    }

    protected Object performExecute(Map parameters)
    {
        ManagedConnection mconn = getStoreManager().getConnection(ec);
        List results = new ArrayList();
        try
        {
            GraphDatabaseService db = (GraphDatabaseService) mconn.getConnection();

            long startTime = System.currentTimeMillis();
            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021046", "Cypher", getSingleStringQuery(), null));
            }

            results = Neo4jUtils.executeCypherQuery(this, db, cypher, null);

            if (NucleusLogger.QUERY.isDebugEnabled())
            {
                NucleusLogger.QUERY.debug(Localiser.msg("021074", "Cypher", "" + (System.currentTimeMillis() - startTime)));
            }

            return results;
        }
        finally
        {
            mconn.release();
        }
    }
}
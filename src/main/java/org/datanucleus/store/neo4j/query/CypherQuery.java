package org.datanucleus.store.neo4j.query;

import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neo4j.EmbeddedQueryEngine;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.neo4j.graphdb.GraphDatabaseService;

public class CypherQuery extends AbstractJavaQuery<Object> {
    private static final long serialVersionUID = 2808968696540162104L;
    private final String cypher;

    public CypherQuery(StoreManager storeMgr, ExecutionContext ec) {
        this(storeMgr, ec, (String) null);
    }

    public CypherQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec);
        this.cypher = query;
    }

    @Override
    public String getSingleStringQuery() { return cypher; }
    @Override
    public void compileGeneric(Map p) {}
    @Override
    protected void compileInternal(Map p) {}

    @Override
    protected Object performExecute(Map parameters) {
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try {
            GraphDatabaseService db = (GraphDatabaseService) mconn.getConnection();
            return EmbeddedQueryEngine.executeCypherQuery(this, db, cypher, parameters);
        } finally {
            mconn.release();
        }
    }
}
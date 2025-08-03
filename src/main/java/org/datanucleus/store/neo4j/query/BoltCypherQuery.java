package org.datanucleus.store.neo4j.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.neo4j.BoltConnectionFactoryImpl;
import org.datanucleus.store.neo4j.BoltPersistenceUtils;
import org.datanucleus.store.query.AbstractJavaQuery;
import org.datanucleus.store.query.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;

/**
 * Final, corrected Cypher query implementation for the Neo4j Bolt driver.
 * This version overrides `executeQuery` to bypass the faulty logic in the DataNucleus Query base class,
 * thus preventing the Long/Collection ClassCastException.
 */
public class BoltCypherQuery extends AbstractJavaQuery<Object> {
    private final String cypher;

    public BoltCypherQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec);
        this.cypher = query;
    }

    public BoltCypherQuery(StoreManager storeMgr, ExecutionContext ec, Query q) {
        super(storeMgr, ec);
        this.cypher = (q instanceof AbstractJavaQuery) ? ((AbstractJavaQuery)q).getSingleStringQuery() : null;
    }

    @Override
    public String getSingleStringQuery() { return cypher; }
    @Override
    public void compileGeneric(Map p) {}
    @Override
    protected void compileInternal(Map p) {}

    /**
     * === THE DEFINITIVE FIX IS HERE ===
     * We override the faulty `executeQuery` method from the superclass.
     * This allows us to control the return type and bypass the bug that causes the ClassCastException.
     */
    @Override
    protected Object executeQuery(final Map<Object, Object> parameters) {
        boolean isReturnQuery = (cypher != null && cypher.toUpperCase().contains("RETURN"));

        if (isReturnQuery) {
            // This is a SELECT query, the superclass logic is fine for this path.
            return super.executeQuery(parameters);
        }

        // This is a BULK (DELETE/UPDATE) query. We execute it but return the Long directly,
        // bypassing the superclass's attempt to cast it to a Collection.
        prepareDatastore();
        return performExecute(parameters);
    }

    @Override
    protected Object performExecute(Map parameters) {
        ManagedConnection mconn = getStoreManager().getConnectionManager().getConnection(ec);
        try {
            BoltConnectionFactoryImpl.EmulatedXAResource xaRes = (BoltConnectionFactoryImpl.EmulatedXAResource) mconn.getXAResource();
            Transaction tx = xaRes.getTransaction();

            boolean isReturnQuery = (cypher != null && cypher.toUpperCase().contains("RETURN"));
            if (isReturnQuery) {
                if (this.resultClass == null) { throw new NucleusException("SELECT query needs a resultClass."); }
                Result result = tx.run(cypher, parameters);
                List<Object> pcResults = new ArrayList<>();
                AbstractClassMetaData cmd = ec.getMetaDataManager().getMetaDataForClass(this.resultClass, ec.getClassLoaderResolver());
                while (result.hasNext()) {
                    Record record = result.next();
                    Value val = record.get(0);
                    if (!val.isNull()) {
                        Object pc = BoltPersistenceUtils.getObjectForNode(ec, val.asNode(), cmd);
                        pcResults.add(pc);
                    }
                }
                return pcResults;
            }
            
            Result result = tx.run(cypher, parameters);
            ResultSummary summary = result.consume();
            return (long) summary.counters().nodesCreated() + (long) summary.counters().nodesDeleted();
        } catch (Exception e) {
             throw new NucleusException("Error in BoltCypherQuery: " + e.getMessage(), e);
        } finally {
            mconn.release();
        }
    }
}
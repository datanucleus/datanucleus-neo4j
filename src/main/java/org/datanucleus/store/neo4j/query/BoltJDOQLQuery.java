package org.datanucleus.store.neo4j.query;

import java.util.Map;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.query.Query;
import org.datanucleus.store.query.AbstractJDOQLQuery;
import org.datanucleus.store.query.AbstractJavaQuery;

/**
 * A placeholder query class for remote Bolt connections.
 * The original JDOQLQuery is tied to the embedded driver, so we use this
 * to provide a clear error message instead of a NoClassDefFoundError.
 */
public class BoltJDOQLQuery extends AbstractJDOQLQuery {
    private static final long serialVersionUID = 1L;

    public BoltJDOQLQuery(StoreManager storeMgr, ExecutionContext ec) {
        super(storeMgr, ec);
    }
    public BoltJDOQLQuery(StoreManager storeMgr, ExecutionContext ec, String query) {
        super(storeMgr, ec, query);
    }
    
    /**
     * THIS IS THE FIX.
     * The constructor now correctly handles being created from another query.
     * It checks if the other query is of a compatible type and uses the correct superclass constructor.
     */
    public BoltJDOQLQuery(StoreManager storeMgr, ExecutionContext ec, Query query) {
        super(storeMgr, ec, (query instanceof AbstractJDOQLQuery) ? (AbstractJDOQLQuery)query : null);
        if (!(query instanceof AbstractJDOQLQuery)) {
            // If we are trying to create from an incompatible query type, copy the string form.
            if (query instanceof AbstractJavaQuery) {
                this.singleString = ((AbstractJavaQuery)query).getSingleStringQuery();
            }
        }
    }

    @Override
    protected void compileInternal(Map params) {
        throw new NucleusUserException("JDOQL/JPQL is not supported with this custom remote Bolt implementation. Please use native Cypher queries.");
    }
    
    @Override
    protected Object performExecute(Map params) {
        // This will never be called because compileInternal will throw an exception first.
        return null;
    }
}
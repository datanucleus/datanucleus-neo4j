package org.datanucleus.store.neo4j;

import java.io.File;
import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PropertyNames;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class ConnectionFactoryImpl extends AbstractConnectionFactory {
    private final boolean isRemote;
    private final BoltConnectionFactoryImpl boltFactoryDelegate;
    private GraphDatabaseService graphDB;

    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType) {
        super(storeMgr, resourceType);
        String url = storeMgr.getConnectionURL();
        if (url != null && (url.toLowerCase().contains("bolt"))) {
            this.isRemote = true;
            this.boltFactoryDelegate = new BoltConnectionFactoryImpl(storeMgr, resourceType);
        } else {
            this.isRemote = false;
            this.boltFactoryDelegate = null;
        }
    }

    public ManagedConnection createManagedConnection(ExecutionContext ec, Map options) {
        if (isRemote) {
            return boltFactoryDelegate.createManagedConnection(ec, options);
        } else {
            initialiseGraphDB(); 
            return new ManagedConnectionImpl();
        }
    }

    private synchronized void initialiseGraphDB() {
        if (graphDB != null) return;
        String url = storeMgr.getConnectionURL();
        if (url == null) throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "'");
        String remains = url.substring(6).trim();
        if (remains.indexOf(':') == 0) remains = remains.substring(1);
        String dbName = "DataNucleus";
        if (remains.length() > 0) dbName = remains;
        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        String propsFileName = storeMgr.getStringProperty("datanucleus.ConnectionPropertiesFile");
        try {
            if (StringUtils.isWhitespace(propsFileName)) {
                graphDB = factory.newEmbeddedDatabase(new File(dbName));
            } else {
                File propsFile = new File(propsFileName);
                if (!propsFile.exists()) {
                    graphDB = factory.newEmbeddedDatabase(new File(dbName));
                } else {
                    graphDB = factory.newEmbeddedDatabaseBuilder(new File(dbName)).loadPropertiesFromFile(propsFileName).newGraphDatabase();
                }
            }
        } catch (Exception e) {
            throw new NucleusException("Exception creating embedded Neo4j database", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> graphDB.shutdown()));
    }

    public void close() {
        if (boltFactoryDelegate != null) { /* Delegate handles its own closing */ }
        if (graphDB != null) {
            graphDB.shutdown();
        }
        super.close();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection {
        Transaction graphTx;
        XAResource xaRes = null;
        public ManagedConnectionImpl() {}
        @Override
        public boolean closeAfterTransactionEnd() { return false; }
        protected void obtainNewConnection() {
            if (conn == null) {
                conn = graphDB;
                graphTx = graphDB.beginTx();
            }
            if (graphTx == null) {
                graphTx = ((GraphDatabaseService)conn).beginTx();
            }
        }
        public Object getConnection() {
            if (conn == null || graphTx == null) obtainNewConnection();
            return conn;
        }
        public void release() {
            if (commitOnRelease) {
                if (conn != null) {
                    graphTx.success();
                    graphTx.close();
                    graphTx = null;
                    xaRes = null;
                }
            }
            super.release();
        }
        public void close() {
            if (conn == null) return;
            for (int i=0; i<listeners.size(); i++) listeners.get(i).managedConnectionPreClose();
            if (graphTx != null) {
                graphTx.success();
                graphTx.close();
                graphTx = null;
                xaRes = null;
            }
            for (int i=0; i<listeners.size(); i++) listeners.get(i).managedConnectionPostClose();
            xaRes = null;
            super.close();
        }
        public XAResource getXAResource() {
            if (xaRes == null) {
                if (conn == null || graphTx == null) obtainNewConnection();
                xaRes = new EmulatedXAResource(this);
            }
            return xaRes;
        }
    }

    static class EmulatedXAResource extends AbstractEmulatedXAResource {
        Transaction graphTx;
        EmulatedXAResource(ManagedConnectionImpl mconn) {
            super(mconn);
            this.graphTx = mconn.graphTx;
        }
        public void commit(Xid xid, boolean onePhase) throws XAException {
            super.commit(xid, onePhase);
            graphTx.success();
            graphTx.close();
            ((ManagedConnectionImpl)mconn).graphTx = null;
            ((ManagedConnectionImpl)mconn).xaRes = null;
        }
        public void rollback(Xid xid) throws XAException {
            super.rollback(xid);
            graphTx.failure();
            graphTx.close();
            ((ManagedConnectionImpl)mconn).graphTx = null;
            ((ManagedConnectionImpl)mconn).xaRes = null;
        }
        public void end(Xid xid, int flags) throws XAException {
            super.end(xid, flags);
            ((ManagedConnectionImpl)mconn).xaRes = null;
        }
    }
}
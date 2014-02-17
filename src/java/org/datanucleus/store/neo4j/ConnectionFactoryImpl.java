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
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Implementation of a ConnectionFactory for Neo4j.
 * Accepts a URL of the form 
 * <pre>neo4j:{db_path}</pre>
 * If {db_path} is not specified then will use "datanucleus" as the DB_PATH.
 * Obtains the GraphDbService when initialising the ConnectionFactory and starts/finishes transactions for
 * each ExecutionContext. In Neo4j a thread has its own transaction which doesn't map directly onto
 * an ExecutionContext
 */
public class ConnectionFactoryImpl extends AbstractConnectionFactory
{
    protected static final Localiser LOCALISER = Localiser.getInstance(
        "org.datanucleus.store.neo4j.Localisation", Neo4jStoreManager.class.getClassLoader());

    GraphDatabaseService graphDB;

    /**
     * Constructor.
     * @param storeMgr Store Manager
     * @param resourceType Type of resource (tx, nontx)
     */
    public ConnectionFactoryImpl(StoreManager storeMgr, String resourceType)
    {
        super(storeMgr, resourceType);

        // "neo4j:[db_path]"
        String url = storeMgr.getConnectionURL();
        if (url == null)
        {
            throw new NucleusException("You haven't specified persistence property '" + PropertyNames.PROPERTY_CONNECTION_URL + "' (or alias)");
        }

        String remains = url.substring(6).trim();
        if (remains.indexOf(':') == 0)
        {
            remains = remains.substring(1);
        }

        // Assumed to be DB_PATH
        String dbName = "DataNucleus";
        if (remains.length() > 0)
        {
            dbName = remains;
        }
        if (dbName.startsWith("http:"))
        {
            // TODO Support java-rest-binding to connect to remote databases
            throw new NucleusException("Neo4j database name starts with http - do not currently support connecting to remote databases");
        }

        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        String propsFileName = storeMgr.getStringProperty("datanucleus.ConnectionPropertiesFile");
        if (StringUtils.isWhitespace(propsFileName))
        {
            graphDB = factory.newEmbeddedDatabase(dbName);
        }
        else
        {
            File propsFile = new File(propsFileName);
            if (!propsFile.exists())
            {
                NucleusLogger.CONNECTION.warn("Connection properties file " + propsFileName + " doesn't exist! Ignoring and creating database using defaults");
                graphDB = factory.newEmbeddedDatabase(dbName);
            }
            else
            {
                graphDB = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbName).loadPropertiesFromFile(propsFileName).newGraphDatabase();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                graphDB.shutdown();
            }
        });
    }

    public void close()
    {
        super.close();
    }

    /**
     * Obtain a connection from the Factory. The connection will be enlisted within the transaction
     * associated to the ExecutionContext
     * @param ec the pool that is bound the connection during its lifecycle (or null)
     * @param txnOptionsIgnored Any options for creating the connection
     * @return the {@link org.datanucleus.store.connection.ManagedConnection}
     */
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map txnOptionsIgnored)
    {
        return new ManagedConnectionImpl();
    }

    public class ManagedConnectionImpl extends AbstractManagedConnection
    {
        Transaction graphTx;
        XAResource xaRes = null;

        public ManagedConnectionImpl()
        {
        }

        /* (non-Javadoc)
         * @see org.datanucleus.store.connection.AbstractManagedConnection#closeAfterTransactionEnd()
         */
        @Override
        public boolean closeAfterTransactionEnd()
        {
            // Don't call close() immediately after transaction commit/rollback/end since we want to
            // hang on to the connection until the ExecutionContext ends
            return false;
        }

        protected void obtainNewConnection()
        {
            if (conn == null)
            {
                // Set the "connection" to the graphDB, and start its transaction
                conn = graphDB;
                graphTx = graphDB.beginTx();
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is starting");
            }
            if (graphTx == null)
            {
                // Make sure the graphTx is started
                graphTx = ((GraphDatabaseService)conn).beginTx();
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is starting");
            }
        }

        public Object getConnection()
        {
            if (conn == null || graphTx == null)
            {
                // Set the "connection" to the graphDB, and start its transaction
                obtainNewConnection();
            }
            return conn;
        }

        public void release()
        {
            if (commitOnRelease)
            {
                if (conn != null)
                {
                    NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
                    graphTx.success();
                    graphTx.finish();
                    graphTx = null;
                    xaRes = null;
                    NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");
                }
            }
            super.release();
        }

        public void close()
        {
            if (conn == null)
            {
                return;
            }

            // Notify anything using this connection to use it now
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPreClose();
            }

            if (graphTx != null)
            {
                // End the current request
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " is committing");
                graphTx.success();
                graphTx.finish();
                graphTx = null;
                xaRes = null;
                NucleusLogger.CONNECTION.debug("Managed connection " + this.toString() + " committed connection");
            }

            // Remove the connection from pooling
            for (int i=0; i<listeners.size(); i++)
            {
                ((ManagedConnectionResourceListener)listeners.get(i)).managedConnectionPostClose();
            }

            conn = null;
        }

        public XAResource getXAResource()
        {
            if (xaRes == null)
            {
                if (conn == null || graphTx == null)
                {
                    // Make sure we have a connection and graphTx
                    obtainNewConnection();
                }
                xaRes = new EmulatedXAResource(this);
            }
            return xaRes;
        }
    }

    /**
     * Emulate the two phase protocol for non XA
     */
    static class EmulatedXAResource implements XAResource
    {
        ManagedConnectionImpl mconn;
        Transaction graphTx;

        EmulatedXAResource(ManagedConnectionImpl mconn)
        {
            this.mconn = mconn;
            this.graphTx = mconn.graphTx;
        }

        public void start(Xid xid, int flags) throws XAException
        {
        }

        public void commit(Xid xid, boolean onePhase) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is committing for transaction "+xid.toString()+" with onePhase="+onePhase);
            graphTx.success();
            graphTx.finish();
            mconn.graphTx = null;
            mconn.xaRes = null;
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " committed connection for transaction "+xid.toString()+" with onePhase="+onePhase);
        }

        public void rollback(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is rolling back for transaction "+xid.toString());
            graphTx.failure();
            graphTx.finish();
            mconn.graphTx = null;
            mconn.xaRes = null;
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " rolled back connection for transaction "+xid.toString());
        }

        public void end(Xid xid, int flags) throws XAException
        {
            mconn.xaRes = null;
        }

        public void forget(Xid xid) throws XAException
        {
        }

        public int prepare(Xid xid) throws XAException
        {
            NucleusLogger.CONNECTION.debug("Managed connection "+this.toString()+
                " is preparing for transaction "+xid.toString());
            return 0;
        }

        public Xid[] recover(int flags) throws XAException
        {
            throw new XAException("Unsupported operation");
        }

        public int getTransactionTimeout() throws XAException
        {
            return 0;
        }

        public boolean setTransactionTimeout(int timeout) throws XAException
        {
            return false;
        }

        public boolean isSameRM(XAResource xares) throws XAException
        {
            return (this == xares);
        }
    }
}
/**********************************************************************
Copyright (c) 2024 Andy Jefferson and others. All rights reserved.
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

import java.util.Map;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.AbstractConnectionFactory;
import org.datanucleus.store.connection.AbstractEmulatedXAResource;
import org.datanucleus.store.connection.AbstractManagedConnection;
import org.datanucleus.store.connection.ManagedConnection;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;

public class BoltConnectionFactoryImpl extends AbstractConnectionFactory {

    private final Driver driver;

    public BoltConnectionFactoryImpl(StoreManager storeMgr, String resourceType) {
        super(storeMgr, resourceType);

        String url = storeMgr.getConnectionURL();
        String user = storeMgr.getConnectionUserName();
        String password = storeMgr.getConnectionPassword();

        // CRITICAL FIX: The Neo4j driver expects "bolt://" or "neo4j://", not "neo4j:bolt://"
        String driverUrl = url;
        if (url != null && url.toLowerCase().startsWith("neo4j:")) {
            driverUrl = url.substring("neo4j:".length());
        }

        this.driver = GraphDatabase.driver(driverUrl, AuthTokens.basic(user, password));
    }

    @Override
    public ManagedConnection createManagedConnection(ExecutionContext ec, Map<String, Object> options) {
        return new BoltManagedConnection();
    }

    @Override
    public void close() {
        if (driver != null) {
            driver.close();
        }
        super.close();
    }

    public class BoltManagedConnection extends AbstractManagedConnection {
        private Session session;
        private EmulatedXAResource xaRes;

        BoltManagedConnection() {
            this.session = driver.session();
        }

        Session getOrOpenSession() {
            if (session == null || !session.isOpen()) {
                session = driver.session();
            }
            return session;
        }

        @Override
        public Object getConnection() {
            // Always guarantees an open session
            return getOrOpenSession();
        }

        @Override
        public XAResource getXAResource() {
            if (xaRes == null) {
                xaRes = new EmulatedXAResource(this);
            }
            return xaRes;
        }

        @Override
        public void release() {
            // IMPORTANT:
            // DataNucleus may call release() between operations in the same PM/txn lifecycle.
            // If you close the session here, you’ll get "Session is closed" later.
            // So either do nothing here *or* expect to always reopen in getConnection() (which we do).
            // We'll *not* close here.
        }

        @Override
        public void close() {
            release();
            super.close();
        }
    }

    public static class EmulatedXAResource extends AbstractEmulatedXAResource {
        private Transaction tx;

        public EmulatedXAResource(ManagedConnection mconn) {
            super(mconn);
        }

        private BoltManagedConnection boltMC() {
            return (BoltManagedConnection) mconn;
        }

        public synchronized Transaction getTransaction() {
            if (tx == null || !tx.isOpen()) {
                Session session = boltMC().getOrOpenSession();
                if (session == null || !session.isOpen()) {
                    throw new IllegalStateException("The Neo4j Session is closed. Cannot begin a new transaction.");
                }
                tx = session.beginTransaction();
            }
            return tx;
        }

        @Override
        public void commit(Xid xid, boolean onePhase) throws XAException {
            try {
                if (tx != null && tx.isOpen()) {
                    tx.commit();
                }
            } catch (RuntimeException e) {
                // Ensure we null tx and translate the exception to XA
                tx = null;
                throw (XAException) new XAException(XAException.XA_RBROLLBACK).initCause(e);
            } finally {
                tx = null;
                super.commit(xid, onePhase); // This is critical, it calls mconn.release()
            }
            super.commit(xid, onePhase);
        }

        @Override
        public void rollback(Xid xid) throws XAException {
            try {
                if (tx != null && tx.isOpen()) {
                    tx.rollback();
                }
            } catch (RuntimeException e) {
                tx = null;
                throw (XAException) new XAException(XAException.XA_RBROLLBACK).initCause(e);
            } finally {
                tx = null;
            }
            super.rollback(xid);
        }

        @Override
        public void end(Xid xid, int flags) throws XAException {
            // Optional: if DN calls end() to signify end of work, you could also close tx here when appropriate
            // but DN typically coordinates via commit/rollback in AbstractEmulatedXAResource.
            super.end(xid, flags);
        }
    }
}
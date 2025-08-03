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

Contributors :
    ...
***********************************************************************/
package org.datanucleus.store.neo4j.query;

import org.datanucleus.ExecutionContext;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.connection.ManagedConnectionResourceListener;
import org.datanucleus.store.query.QueryResult;

/**
 * A reusable listener to disconnect a query result from its connection when the transaction or connection closes.
 */
public class QueryConnectionListener implements ManagedConnectionResourceListener {

    private final QueryResult qr;
    private final ManagedConnection mconn;
    private final ExecutionContext ec;

    public QueryConnectionListener(QueryResult qr, ManagedConnection mconn, ExecutionContext ec) {
        this.qr = qr;
        this.mconn = mconn;
        this.ec = ec;
    }

    @Override
    public void managedConnectionPreClose() {
        if (!ec.getTransaction().isActive()) {
            qr.disconnect();
        }
    }

    @Override
    public void transactionPreClose() {
        qr.disconnect();
    }
    
    @Override
    public void resourcePostClose() {
        mconn.removeListener(this);
    }

    @Override
    public void managedConnectionPostClose() {}

    @Override
    public void transactionFlushed() {}
}
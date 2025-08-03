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
package org.datanucleus.store.neo4j.query;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.neo4j.Neo4jObjectFactory;
import org.datanucleus.store.neo4j.EmbeddedPersistenceUtils;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.ConcurrentReferenceHashMap;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.ConcurrentReferenceHashMap.ReferenceType;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

/**
 * QueryResult for Neo4j queries that lazily loads results from the provided Result object.
 */
public class LazyLoadQueryResult extends AbstractQueryResult
{
    private static final long serialVersionUID = 1L;

    protected transient ExecutionContext ec;
    protected transient Result result;
    protected String candidateAliasName;
    protected AbstractClassMetaData cmd;
    protected String[] cypherResults;

    protected Map<Integer, Object> itemsByIndex = null;

    public LazyLoadQueryResult(Query q, Result result, String cypherResult)
    {
        super(q);
        this.ec = q.getExecutionContext();
        this.cmd = (q.getCandidateClass() != null) ? ec.getMetaDataManager().getMetaDataForClass(query.getCandidateClass(), ec.getClassLoaderResolver()) : null;
        this.result = result;
        this.cypherResults = (cypherResult != null ? cypherResult.trim().split("\\s*,\\s*") : null);
        this.candidateAliasName = (query.getCompilation() != null) ? query.getCompilation().getCandidateAlias() : null;
        
        this.resultSizeMethod = "last"; 

        String cacheType = query.getStringExtensionProperty("cacheType", "soft");
        if ("soft".equalsIgnoreCase(cacheType)) {
            itemsByIndex = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.SOFT);
        } else if ("weak".equalsIgnoreCase(cacheType)) {
            itemsByIndex = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.WEAK);
        } else if ("strong".equalsIgnoreCase(cacheType)) {
            itemsByIndex = new HashMap<>();
        } else {
            itemsByIndex = new ConcurrentReferenceHashMap<>(1, ReferenceType.STRONG, ReferenceType.SOFT);
        }
    }

    @Override
    protected void closingConnection()
    {
        if (loadResultsAtCommit && isOpen() && result != null)
        {
            NucleusLogger.QUERY.info(Localiser.msg("052606", query.toString()));
            while (result.hasNext())
            {
                Map<String, Object> map = result.next();
                if (itemsByIndex != null) {
                    itemsByIndex.put(itemsByIndex.size(), getResultFromMapRow(map));
                }
            }
            result.close();
            result = null;
        }
    }

    private Object getResultFromMapRow(Map<String, Object> map)
    {
        boolean isCandidateResult = (cypherResults == null || (candidateAliasName != null && cypherResults.length == 1 && cypherResults[0].equals(candidateAliasName)));

        if (isCandidateResult)
        {
            if (candidateAliasName == null || !map.containsKey(candidateAliasName)) {
                return null; 
            }
            PropertyContainer container = (PropertyContainer) map.get(candidateAliasName);
            if (container == null) return null;

            AbstractClassMetaData propObjCmd = EmbeddedPersistenceUtils.getClassMetaDataForPropertyContainer(container, ec, cmd);
            return Neo4jObjectFactory.getObjectForPropertyContainer(container, propObjCmd, ec);
        }
        else
        {
            if (cypherResults.length == 1)
            {
                return map.get(cypherResults[0].trim());
            }
            else
            {
                Object[] row = new Object[cypherResults.length];
                for (int i = 0; i < cypherResults.length; i++)
                {
                    row[i] = map.get(cypherResults[i].trim());
                }
                return row;
            }
        }
    }

    @Override
    public synchronized void close()
    {
        if (itemsByIndex != null) {
            itemsByIndex.clear();
        }
        if (result != null) {
            result.close();
            result = null;
        }
        super.close();
    }

    @Override
    protected void closeResults()
    {
    }

    @Override
    protected int getSizeUsingMethod()
    {
        if ("last".equalsIgnoreCase(resultSizeMethod))
        {
            while (getNextObject() != null) {
                // Keep iterating to populate cache
            }
            size = (itemsByIndex != null) ? itemsByIndex.size() : 0;
            return size;
        }
        return super.getSizeUsingMethod();
    }

    @Override
    public Object get(int index)
    {
        if (index < 0)
        {
            throw new IndexOutOfBoundsException("Index must be 0 or higher");
        }
        if (itemsByIndex != null && itemsByIndex.containsKey(index))
        {
            return itemsByIndex.get(index);
        }

        while (true)
        {
            Object nextObj = getNextObject();
            if (itemsByIndex != null && itemsByIndex.size() == (index + 1))
            {
                return nextObj;
            }
            if (result == null) 
            {
                throw new IndexOutOfBoundsException("Index " + index + " is out of bounds for the result set of size " + (itemsByIndex != null ? itemsByIndex.size() : 0));
            }
        }
    }

    protected synchronized Object getNextObject()
    {
        if (result == null)
        {
            return null;
        }

        if (result.hasNext())
        {
            Map<String, Object> map = result.next();
            Object rowResult = getResultFromMapRow(map);
            if (itemsByIndex != null)
            {
                itemsByIndex.put(itemsByIndex.size(), rowResult);
            }
            return rowResult;
        }
        else
        {
            result.close();
            result = null;
            return null;
        }
    }

    @Override
    public Iterator<Object> iterator()
    {
        return new QueryResultIterator();
    }

    @Override
    public ListIterator<Object> listIterator()
    {
        return new QueryResultIterator();
    }

    private class QueryResultIterator extends AbstractQueryResultIterator
    {
        private int cursor = -1;

        @Override
        public boolean hasNext()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!isOpen()) return false;
                if (cursor + 1 < (itemsByIndex != null ? itemsByIndex.size() : 0)) return true;
                return (result != null && result.hasNext());
            }
        }

        @Override
        public Object next()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!hasNext())
                {
                     throw new NoSuchElementException(Localiser.msg("052602"));
                }
                cursor++;
                return get(cursor);
            }
        }
        
        // === FIX: Implemented all required abstract methods from ListIterator ===
        @Override
        public boolean hasPrevious()
        {
            return cursor > 0;
        }

        @Override
        public Object previous()
        {
            if (!hasPrevious()) {
                 throw new NoSuchElementException();
            }
            return get(--cursor);
        }

        @Override
        public int nextIndex()
        {
            return cursor + 1;
        }

        @Override
        public int previousIndex()
        {
            return cursor;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) return true;
        if (!(o instanceof LazyLoadQueryResult)) return false;
        LazyLoadQueryResult other = (LazyLoadQueryResult) o;
        return this.query == other.query;
    }
    
    // === FIX: Removed @Override annotation as the method is not in the superclass ===
    protected Object writeReplace() throws ObjectStreamException
    {
        disconnect();
        List<Object> list = new ArrayList<>();
        if (itemsByIndex != null) {
            // Ensure all results are loaded before creating the list for serialization
            if (result != null) {
                size();
            }
            for (int i = 0; i < itemsByIndex.size(); i++) {
                list.add(itemsByIndex.get(i));
            }
        }
        return list;
    }
}
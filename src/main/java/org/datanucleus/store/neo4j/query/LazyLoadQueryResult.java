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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.store.neo4j.Neo4jUtils;
import org.datanucleus.store.query.AbstractQueryResult;
import org.datanucleus.store.query.AbstractQueryResultIterator;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.datanucleus.util.SoftValueMap;
import org.datanucleus.util.StringUtils;
import org.datanucleus.util.WeakValueMap;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Result;

/**
 * QueryResult for Neo4j queries that tries to lazy load results from the provided ExecutionResult
 * so to avoid problems with memory. By default if the query is for instances of a candidate (i.e no result
 * clause) then the method used to calculate the size is by doing a Cypher "count" query, and if the query is
 * for a result clause then the method used to calculate the size is by loading all results; obviously the user
 * can set it through a query extension/hint.
 * 
 * TODO Support fetching objects in batches
 */
public class LazyLoadQueryResult extends AbstractQueryResult
{
    protected ExecutionContext ec;

    protected Result result;

    protected String candidateAliasName;

    protected AbstractClassMetaData cmd;

    protected String[] cypherResults;

    /** Map of object, keyed by the index (0, 1, etc). */
    protected Map<Integer, Object> itemsByIndex = null;

    public LazyLoadQueryResult(Query q, Result result, String cypherResult)
    {
        super(q);
        this.candidateAliasName = query.getCompilation().getCandidateAlias();
        this.ec = q.getExecutionContext();
        this.cmd = ec.getMetaDataManager().getMetaDataForClass(query.getCandidateClass(), ec.getClassLoaderResolver());
        this.result = result;
        this.cypherResults = (cypherResult != null ? cypherResult.split(",") : null);

        if (cypherResults == null || (candidateAliasName != null && cypherResults[0].equals(candidateAliasName)))
        {
            resultSizeMethod = "count";
        }
        else
        {
            resultSizeMethod = "last";
        }
        resultSizeMethod = "last"; // Override the above til we have confidence in count

        // Process any supported extensions
        String cacheType = query.getStringExtensionProperty("cacheType", "strong");
        if (cacheType != null)
        {
            if (cacheType.equalsIgnoreCase("soft"))
            {
                itemsByIndex = new SoftValueMap();
            }
            else if (cacheType.equalsIgnoreCase("weak"))
            {
                itemsByIndex = new WeakValueMap();
            }
            else if (cacheType.equalsIgnoreCase("strong"))
            {
                itemsByIndex = new HashMap();
            }
            else if (cacheType.equalsIgnoreCase("none"))
            {
                itemsByIndex = null;
            }
            else
            {
                itemsByIndex = new WeakValueMap();
            }
        }
        else
        {
            itemsByIndex = new WeakValueMap();
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#closingConnection()
     */
    @Override
    protected void closingConnection()
    {
        resultSizeMethod = "last";
        if (loadResultsAtCommit && isOpen() && result != null)
        {
            // Query connection closing message
            NucleusLogger.QUERY.info(Localiser.msg("052606", query.toString()));

            synchronized (this)
            {
                // Go through to end of Iterator
                while (result.hasNext())
                {
                    Map<String, Object> map = result.next();
                    Object result = getResultFromMapRow(map);
                    itemsByIndex.put(itemsByIndex.size(), result);
                }
                result.close();
                result = null;
            }
        }
    }

    private Object getResultFromMapRow(Map<String, Object> map)
    {
        Object result = null;
        if (cypherResults == null || (candidateAliasName != null && cypherResults[0].equals(candidateAliasName)))
        {
            // Candidate result
            PropertyContainer node = (PropertyContainer) map.get(candidateAliasName);
            AbstractClassMetaData propObjCmd = Neo4jUtils.getClassMetaDataForPropertyContainer(node, query.getExecutionContext(), cmd);
            result = Neo4jUtils.getObjectForPropertyContainer(node, propObjCmd, query.getExecutionContext(), query.getIgnoreCache());
        }
        else
        {
            // Result clause specified, so extract into Object or Object[]
            if (cypherResults.length == 1)
            {
                result = map.get(cypherResults[0]);
            }
            else
            {
                result = new Object[cypherResults.length];
                for (int i=0;i<cypherResults.length;i++)
                {
                    Array.set(result, i, map.get(cypherResults[i]));
                }
            }
        }

        return result;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#close()
     */
    @Override
    public synchronized void close()
    {
        itemsByIndex.clear();
        itemsByIndex = null;
        result = null;

        super.close();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#closeResults()
     */
    @Override
    protected void closeResults()
    {
        // TODO Cache any query results if required
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#getSizeUsingMethod()
     */
    @Override
    protected int getSizeUsingMethod()
    {
        if (resultSizeMethod.equalsIgnoreCase("LAST"))
        {
            // Just load all results and the size is the number we have
            while (true)
            {
                getNextObject();
                if (result == null)
                {
                    size = itemsByIndex.size();
                    return size;
                }
            }
        }

        return super.getSizeUsingMethod();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#get(int)
     */
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

        // Load next object continually until we find it
        while (true)
        {
            Object nextPojo = getNextObject();
            if (itemsByIndex.size() == (index+1))
            {
                return nextPojo;
            }
            if (result == null)
            {
                throw new IndexOutOfBoundsException("Beyond size of the results (" + itemsByIndex.size() + ")");
            }
        }
    }

    /**
     * Method to extract the next object from the candidateResults (if there is one).
     * If a result is present, this puts it into "itemsByIndex".
     * Returns null if no more results.
     * @return The next result (or null if no more).
     */
    protected Object getNextObject()
    {
        if (result == null)
        {
            // Already exhausted
            return null;
        }

        Map<String, Object> map = result.next();
        Object rowResult = getResultFromMapRow(map);
        itemsByIndex.put(itemsByIndex.size(), rowResult);

        if (!result.hasNext())
        {
            // Reached end of results, so null the iterator to signify this
            result.close();
            result = null;
        }

        return rowResult;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#iterator()
     */
    @Override
    public Iterator iterator()
    {
        return new QueryResultIterator();
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#listIterator()
     */
    @Override
    public ListIterator listIterator()
    {
        return new QueryResultIterator();
    }

    private class QueryResultIterator extends AbstractQueryResultIterator
    {
        private int nextRowNum = 0;

        @Override
        public boolean hasNext()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling hasNext() on closed Query will return false
                    return false;
                }

                if (nextRowNum < itemsByIndex.size())
                {
                    return true;
                }

                return (result != null && result.hasNext());
            }
        }

        @Override
        public Object next()
        {
            synchronized (LazyLoadQueryResult.this)
            {
                if (!isOpen())
                {
                    // Spec 14.6.7 Calling next() on closed Query will throw NoSuchElementException
                    throw new NoSuchElementException(Localiser.msg("052600"));
                }

                if (nextRowNum < itemsByIndex.size())
                {
                    // Already read in this value so return it
                    Object pojo = itemsByIndex.get(nextRowNum);
                    ++nextRowNum;
                    return pojo;
                }
                else if (result != null && result.hasNext())
                {
                    // Get next value from resultIterator
                    Object pojo = getNextObject();
                    ++nextRowNum;
                    return pojo;
                }
                throw new NoSuchElementException(Localiser.msg("052602"));
            }
        }

        @Override
        public boolean hasPrevious()
        {
            // We only navigate in forward direction, but maybe could provide this method
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int nextIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public Object previous()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        @Override
        public int previousIndex()
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.query.AbstractQueryResult#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o)
    {
        if (o == null || !(o instanceof LazyLoadQueryResult))
        {
            return false;
        }

        LazyLoadQueryResult other = (LazyLoadQueryResult)o;
        // TODO Base equality on the results too (itemsByIndex) but means we have to load them
        if (query != null)
        {
            return other.query == query;
        }
        return StringUtils.toJVMIDString(other).equals(StringUtils.toJVMIDString(this));
    }

    /**
     * Handle serialisation by returning a java.util.ArrayList of all of the results for this query
     * after disconnecting the query which has the consequence of enforcing the load of all objects.
     * @return The object to serialise
     * @throws ObjectStreamException Thrown if an error occurs
     */
    protected Object writeReplace() throws ObjectStreamException
    {
        disconnect();
        List list = new ArrayList();
        for (int i=0;i<itemsByIndex.size();i++)
        {
            list.add(itemsByIndex.get(i));
        }
        return list;
    }
}
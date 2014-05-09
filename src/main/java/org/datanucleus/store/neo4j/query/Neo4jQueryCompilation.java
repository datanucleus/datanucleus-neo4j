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

/**
 * Datastore-specific (Neo4j Cypher) compilation information for a java query.
 */
public class Neo4jQueryCompilation
{
    String cypherText = null;

    boolean filterComplete = true;

    boolean orderComplete = true;

    boolean resultComplete = true;

    boolean rangeComplete = true;

    boolean precompilable = true;

    public Neo4jQueryCompilation()
    {
    }

    public boolean isPrecompilable()
    {
        return precompilable;
    }

    public void setPrecompilable(boolean flag)
    {
        this.precompilable = flag;
    }

    public void setCypherText(String text)
    {
        this.cypherText = text;
    }

    public String getCypherText()
    {
        return cypherText;
    }

    public boolean isFilterComplete()
    {
        return filterComplete;
    }

    public boolean isResultComplete()
    {
        return resultComplete;
    }

    public boolean isOrderComplete()
    {
        return orderComplete;
    }

    public boolean isRangeComplete()
    {
        return rangeComplete;
    }

    public void setFilterComplete(boolean complete)
    {
        this.filterComplete = complete;
    }

    public void setOrderComplete(boolean complete)
    {
        this.orderComplete = complete;
    }

    public void setResultComplete(boolean complete)
    {
        this.resultComplete = complete;
    }

    public void setRangeComplete(boolean complete)
    {
        this.rangeComplete = complete;
    }
}
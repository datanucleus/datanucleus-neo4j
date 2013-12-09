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
package org.datanucleus.store.neo4j.query.expression;

/**
 * Representation of a literal in a Neo4j query.
 */
public class Neo4jLiteral extends Neo4jExpression
{
    Object value;

    public Neo4jLiteral(Object value)
    {
        this.value = value;
        this.cypherText = toString();
    }

    public Object getValue()
    {
        return value;
    }

    public String toString()
    {
        return "" + value;
    }
}

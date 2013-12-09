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
 * Representation of an aggregate (MAX, MIN, AVG, SUM, COUNT) in Cypher.
 */
public class Neo4jAggregateExpression extends Neo4jExpression
{
    public Neo4jAggregateExpression(String aggregateName, Neo4jExpression fieldExpr)
    {
        if (aggregateName.equalsIgnoreCase("MAX"))
        {
            cypherText = "max(" + fieldExpr.getCypherText() + ")";
        }
        else if (aggregateName.equalsIgnoreCase("MIN"))
        {
            cypherText = "min(" + fieldExpr.getCypherText() + ")";
        }
        else if (aggregateName.equalsIgnoreCase("SUM"))
        {
            cypherText = "sum(" + fieldExpr.getCypherText() + ")";
        }
        else if (aggregateName.equalsIgnoreCase("AVG"))
        {
            cypherText = "avg(" + fieldExpr.getCypherText() + ")";
        }
        else if (aggregateName.equalsIgnoreCase("COUNT"))
        {
            cypherText = "count(" + fieldExpr.getCypherText() + ")";
        }
    }
}

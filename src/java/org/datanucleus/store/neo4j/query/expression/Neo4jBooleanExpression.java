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

import org.datanucleus.query.expression.Expression;

/**
 * Representation of a boolean expression in Neo4j queries.
 */
public class Neo4jBooleanExpression extends Neo4jExpression
{
    public Neo4jBooleanExpression(String propName, Object value, Expression.Operator op)
    {
        String valueStr = "" + value;
        if (value != null && value instanceof String)
        {
            // Quote any strings
            valueStr = "\"" + valueStr + "\"";
        }
        if (op == Expression.OP_EQ)
        {
            cypherText = propName + " = " + valueStr;
        }
        else if (op == Expression.OP_NOTEQ)
        {
            cypherText = propName + " != " + valueStr;
        }
        else if (op == Expression.OP_GT)
        {
            cypherText = propName + " > " + valueStr;
        }
        else if (op == Expression.OP_GTEQ)
        {
            cypherText = propName + " >= " + valueStr;
        }
        else if (op == Expression.OP_LT)
        {
            cypherText = propName + " < " + valueStr;
        }
        else if (op == Expression.OP_LTEQ)
        {
            cypherText = propName + " <= " + valueStr;
        }
    }

    public Neo4jBooleanExpression(Neo4jBooleanExpression expr1, Neo4jBooleanExpression expr2, Expression.DyadicOperator op)
    {
        if (op == Expression.OP_AND)
        {
            cypherText = "(" + expr1.cypherText + ") and (" + expr2.cypherText + ")";
        }
        else if (op == Expression.OP_OR)
        {
            cypherText = "(" + expr1.cypherText + ") or (" + expr2.cypherText + ")";
        }
    }

    public Neo4jBooleanExpression(Neo4jBooleanExpression expr, Expression.MonadicOperator op)
    {
        if (op == Expression.OP_NOT)
        {
            cypherText = "not(" + expr.cypherText + ")";
        }
    }
}

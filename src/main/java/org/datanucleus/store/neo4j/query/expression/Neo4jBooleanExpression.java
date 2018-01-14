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
    public Neo4jBooleanExpression(String cypher)
    {
        cypherText = cypher;
    }

    public Neo4jBooleanExpression(Neo4jFieldExpression fieldExpr, Neo4jLiteral lit, Expression.Operator op)
    {
        String propName = fieldExpr.getFieldName();
        if (op == Expression.OP_EQ)
        {
            cypherText = propName + (lit.getValue() == null ? " IS NULL" : " = " + lit.getCypherText());
        }
        else if (op == Expression.OP_NOTEQ)
        {
            cypherText = propName + (lit.getValue() == null ? " IS NOT NULL" : " <> " + lit.getCypherText());
        }
        else if (op == Expression.OP_GT)
        {
            cypherText = propName + " > " + lit.getCypherText();
        }
        else if (op == Expression.OP_GTEQ)
        {
            cypherText = propName + " >= " + lit.getCypherText();
        }
        else if (op == Expression.OP_LT)
        {
            cypherText = propName + " < " + lit.getCypherText();
        }
        else if (op == Expression.OP_LTEQ)
        {
            cypherText = propName + " <= " + lit.getCypherText();
        }
    }

    public Neo4jBooleanExpression(Neo4jBooleanExpression expr1, Neo4jBooleanExpression expr2, Expression.DyadicOperator op)
    {
        if (op == Expression.OP_AND)
        {
            cypherText = "(" + expr1.getCypherText() + ") and (" + expr2.getCypherText() + ")";
        }
        else if (op == Expression.OP_OR)
        {
            cypherText = "(" + expr1.getCypherText() + ") or (" + expr2.getCypherText() + ")";
        }
    }

    public Neo4jBooleanExpression(Neo4jBooleanExpression expr, Expression.MonadicOperator op)
    {
        if (op == Expression.OP_NOT)
        {
            cypherText = "not(" + expr.getCypherText() + ")";
        }
    }

    public Neo4jBooleanExpression(Neo4jExpression expr1, Neo4jExpression expr2, Expression.Operator op)
    {
        if (op == Expression.OP_EQ)
        {
            cypherText = expr1.getCypherText() + " = " + expr2.getCypherText();
        }
        else if (op == Expression.OP_NOTEQ)
        {
            cypherText = expr1.getCypherText() + " <> " + expr2.getCypherText();
        }
        else if (op == Expression.OP_GT)
        {
            cypherText = expr1.getCypherText() + " > " + expr2.getCypherText();
        }
        else if (op == Expression.OP_GTEQ)
        {
            cypherText = expr1.getCypherText() + " >= " + expr2.getCypherText();
        }
        else if (op == Expression.OP_LT)
        {
            cypherText = expr1.getCypherText() + " < " + expr2.getCypherText();
        }
        else if (op == Expression.OP_LTEQ)
        {
            cypherText = expr1.getCypherText() + " <= " + expr2.getCypherText();
        }
    }
}

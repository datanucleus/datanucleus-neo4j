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

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.store.schema.table.MemberColumnMapping;

/**
 * Expression for a field in a Neo4j query.
 */
public class Neo4jFieldExpression extends Neo4jExpression
{
    MemberColumnMapping mapping;
    AbstractMemberMetaData mmd;
    String fieldName;

    public Neo4jFieldExpression(String fieldName, AbstractMemberMetaData mmd, MemberColumnMapping mapping)
    {
        this.fieldName = fieldName;
        this.cypherText = fieldName;

        this.mmd = mmd;
        this.mapping = mapping;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public MemberColumnMapping getMemberColumnMapping()
    {
        return mapping;
    }

    public AbstractMemberMetaData getMemberMetaData()
    {
        return mmd;
    }
}
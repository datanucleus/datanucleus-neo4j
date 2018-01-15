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
package org.datanucleus.store.neo4j.fieldmanager;

import java.util.ArrayList;
import java.util.List;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.ClassUtils;
import org.neo4j.graphdb.PropertyContainer;

/**
 * FieldManager for putting values from an embedded POJO into a Neo4j Node.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(ObjectProvider op, PropertyContainer propObj, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(op, propObj, insert, table);
        this.mmds = mmds;
    }

    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, PropertyContainer propObj, boolean insert, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(ec, cmd, propObj, insert, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        AbstractMemberMetaData lastMmd = mmds.get(mmds.size()-1);

        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this member being a link back to the owner. TODO Repeat this for nested and their owners
            ObjectProvider[] ownerOPs = ec.getOwnersForEmbeddedObjectProvider(op);
            if (ownerOPs != null && ownerOPs.length == 1 && value != ownerOPs[0].getObject())
            {
                // Make sure the owner field is set
                op.replaceField(fieldNumber, ownerOPs[0].getObject());
            }
            return;
        }

        ExecutionContext ec = op.getExecutionContext();
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, lastMmd))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object - This performs "flat embedding" as fields in the same document
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embCmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                        " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
                }

                if (RelationType.isBidirectional(relationType))
                {
                    // TODO Add logic for bidirectional relations so we know when to stop embedding
                }

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);

                if (value == null)
                {
                    // Remove all properties for this embedded (and sub-embedded objects)
                    int[] embMmdPosns = embCmd.getAllMemberPositions();
                    StoreEmbeddedFieldManager storeEmbFM = new StoreEmbeddedFieldManager(ec, embCmd, propObj, insert, embMmds, table);
                    for (int i=0;i<embMmdPosns.length;i++)
                    {
                        AbstractMemberMetaData embMmd = embCmd.getMetaDataForManagedMemberAtAbsolutePosition(embMmdPosns[i]);
                        if (String.class.isAssignableFrom(embMmd.getType()) || embMmd.getType().isPrimitive() || ClassUtils.isPrimitiveWrapperType(mmd.getTypeName()))
                        {
                            // Remove property for any primitive/wrapper/String fields
                            List<AbstractMemberMetaData> colEmbMmds = new ArrayList<AbstractMemberMetaData>(embMmds);
                            colEmbMmds.add(embMmd);
                            MemberColumnMapping mapping = table.getMemberColumnMappingForEmbeddedMember(colEmbMmds);
                            propObj.removeProperty(mapping.getColumn(0).getName());
                        }
                        else if (Object.class.isAssignableFrom(embMmd.getType()))
                        {
                            storeEmbFM.storeObjectField(embMmdPosns[i], null);
                        }
                    }
                    return;
                }

                // Process all fields of the embedded object
                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                FieldManager ffm = new StoreEmbeddedFieldManager(embOP, propObj, insert, embMmds, table);
                embOP.provideFields(embCmd.getAllMemberPositions(), ffm);
                return;
            }
        }

        storeNonEmbeddedObjectField(mmd, relationType, clr, value);
    }
}

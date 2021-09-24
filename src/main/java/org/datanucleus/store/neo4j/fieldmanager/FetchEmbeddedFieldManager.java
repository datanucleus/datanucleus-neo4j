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
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.graphdb.PropertyContainer;

/**
 * FieldManager for the retrieval of embedded POJO from Neo4j Node.
 */
public class FetchEmbeddedFieldManager extends FetchFieldManager
{
    /** Metadata for the embedded member (maybe nested) that this FieldManager represents). */
    protected List<AbstractMemberMetaData> mmds;

    public FetchEmbeddedFieldManager(DNStateManager sm, PropertyContainer propObj, List<AbstractMemberMetaData> mmds, Table table)
    {
        super(sm, propObj, table);
        this.mmds = mmds;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        EmbeddedMetaData embmd = mmds.get(0).getEmbeddedMetaData();
        if (mmds.size() == 1 && embmd != null && embmd.getOwnerMember() != null && embmd.getOwnerMember().equals(mmd.getName()))
        {
            // Special case of this being a link back to the owner. TODO Repeat this for nested and their owners
            DNStateManager[] ownerSMs = ec.getOwnersForEmbeddedStateManager(sm);
            return (ownerSMs != null && ownerSMs.length > 0 ? ownerSMs[0].getObject() : null);
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded field
            if (RelationType.isRelationSingleValued(relationType))
            {
                /*if (relationType == RelationType.ONE_TO_ONE_BI || relationType == RelationType.MANY_TO_ONE_BI)
                {
                    if ((ownerMmd.getMappedBy() != null && embMmd.getName().equals(ownerMmd.getMappedBy())) ||
                        (embMmd.getMappedBy() != null && ownerMmd.getName().equals(embMmd.getMappedBy())))
                    {
                        // Other side of owner bidirectional, so return the owner
                        StateManager[] ownerSms = op.getEmbeddedOwners();
                        if (ownerSms == null)
                        {
                            throw new NucleusException("Processing of " + embMmd.getFullFieldName() + " cannot set value to owner since owner StateManager not set");
                        }
                        return ownerSms[0].getObject();
                    }
                }*/

                // Check for null value (currently need all columns to return null)
                // TODO Cater for null using embmd.getNullIndicatorColumn etc

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>(mmds);
                embMmds.add(mmd);
                AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embCmd, sm, fieldNumber);
                FieldManager fetchEmbFM = new FetchEmbeddedFieldManager(embSM, propObj, embMmds, table);
                embSM.replaceFields(embCmd.getAllMemberPositions(), fetchEmbFM);
                return embSM.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                NucleusLogger.PERSISTENCE.debug("Field=" + mmd.getFullFieldName() + " not currently supported as embedded into the owning object");
            }
            return null;
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }
}

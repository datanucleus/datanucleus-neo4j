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
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.neo4j.graphdb.PropertyContainer;

/**
 * FieldManager for putting values from an embedded POJO into a Neo4j Node.
 * This class has been refactored to work with its modular parent.
 */
public class StoreEmbeddedFieldManager extends StoreFieldManager {

    /** Metadata for the embedded member (maybe nested) that this FieldManager represents. */
    protected List<AbstractMemberMetaData> mmds;

    public StoreEmbeddedFieldManager(DNStateManager sm, PropertyContainer propContainer, boolean insert, List<AbstractMemberMetaData> mmds, Table table) {
        super(sm, propContainer, insert, table);
        this.mmds = mmds;
    }

    // === FIX: This constructor now correctly calls the existing parent constructor ===
    public StoreEmbeddedFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, PropertyContainer propContainer, boolean insert, List<AbstractMemberMetaData> mmds, Table table) {
        super(ec, cmd, propContainer, insert, table);
        this.mmds = mmds;
    }

    @Override
    protected MemberColumnMapping getColumnMapping(int fieldNumber) {
        List<AbstractMemberMetaData> embMmds = new ArrayList<>(mmds);
        embMmds.add(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
        return table.getMemberColumnMappingForEmbeddedMember(embMmds);
    }

    @Override
    public void storeObjectField(int fieldNumber, Object value) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        
        // Handle link back to owner in embedded objects
        if (mmds.get(0).getEmbeddedMetaData() != null && mmds.get(0).getEmbeddedMetaData().getOwnerMember() != null &&
            mmds.get(0).getEmbeddedMetaData().getOwnerMember().equals(mmd.getName())) {
            // This field is the owner link, no need to store it as a property.
            return;
        }

        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, mmds.get(mmds.size()-1))) {
            // This is a NESTED embedded field.
            List<AbstractMemberMetaData> nestedMmds = new ArrayList<>(mmds);
            nestedMmds.add(mmd);
            
            if (value == null) {
                // TODO: Implement removal of properties for nulled nested embedded objects
                return;
            }

            AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
            DNStateManager embSM = ec.findStateManagerForEmbedded(value, sm, mmd, null);
            FieldManager ffm = new StoreEmbeddedFieldManager(embSM, this.propContainer, insert, nestedMmds, table);
            embSM.provideFields(embCmd.getAllMemberPositions(), ffm);
        } else {
            // === FIX: This is not an embedded field, so delegate to the parent class. ===
            // The parent knows how to handle primitives, wrappers, and relations.
            super.storeObjectField(fieldNumber, value);
        }
    }
}
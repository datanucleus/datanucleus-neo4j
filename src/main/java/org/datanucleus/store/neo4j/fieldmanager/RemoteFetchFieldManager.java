/**********************************************************************
Copyright (c) 2024 Andy Jefferson and others. All rights reserved.
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

import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;

/**
 * Field Manager for retrieving values from a remote Neo4j Node using the Bolt driver.
 * This class has no dependencies on the embedded Neo4j driver.
 */
public class RemoteFetchFieldManager extends AbstractFetchFieldManager {

    private final Node node;

    public RemoteFetchFieldManager(DNStateManager sm, Node node) {
        super(sm);
        this.node = node;
    }

    private String getPropertyName(int fieldNumber) {
        return cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber).getName();
    }

    @Override
    public boolean fetchBooleanField(int fieldNumber) {
        return node.get(getPropertyName(fieldNumber)).asBoolean();
    }

    @Override
    public byte fetchByteField(int fieldNumber) {
        return (byte) node.get(getPropertyName(fieldNumber)).asLong();
    }

    @Override
    public char fetchCharField(int fieldNumber) {
        String str = node.get(getPropertyName(fieldNumber)).asString();
        return (str != null && !str.isEmpty()) ? str.charAt(0) : '\0';
    }

    @Override
    public double fetchDoubleField(int fieldNumber) {
        return node.get(getPropertyName(fieldNumber)).asDouble();
    }

    @Override
    public float fetchFloatField(int fieldNumber) {
        return (float) node.get(getPropertyName(fieldNumber)).asDouble();
    }

    @Override
    public int fetchIntField(int fieldNumber) {
        return node.get(getPropertyName(fieldNumber)).asInt();
    }

    @Override
    public long fetchLongField(int fieldNumber) {
        // === FIX START ===
        // Check if the field we are being asked for is the primary key.
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.isPrimaryKey()) {
            // If it is the PK, return the node's native ID, not a property.
            return node.id();
        }
        // === FIX END ===
        
        // It's a regular long property, so fetch it by name.
        return node.get(getPropertyName(fieldNumber)).asLong();
    }

    @Override
    public short fetchShortField(int fieldNumber) {
        return (short) node.get(getPropertyName(fieldNumber)).asLong();
    }

    @Override
    public String fetchStringField(int fieldNumber) {
        return node.get(getPropertyName(fieldNumber)).asString(null);
    }

    @Override
    public Object fetchObjectField(int fieldNumber) {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT) {
            return sm.provideField(fieldNumber);
        }

        // === FIX START ===
        // Also check for the primary key here, in case it's a Long wrapper type.
        if (mmd.isPrimaryKey()) {
            return node.id();
        }
        // === FIX END ===

        String propName = getPropertyName(fieldNumber);
        Value val = node.get(propName);
        if (val.isNull()) {
            return null;
        }
        
        return val.asObject();
    }
}
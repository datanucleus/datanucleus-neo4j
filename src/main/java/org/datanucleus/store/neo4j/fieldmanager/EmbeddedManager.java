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

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.state.DNStateManager;

/**
 * Manages the persistence of embedded objects as properties on a Neo4j Node.
 * This is currently a placeholder for future implementation.
 */
public final class EmbeddedManager {

    private EmbeddedManager() {
        // Private constructor
    }

    public static void storeEmbeddedField(DNStateManager sm, AbstractMemberMetaData mmd, Object value) {
        // TODO: Implement the logic for storing embedded objects.
        // This would involve:
        // 1. Getting a StateManager for the embedded object.
        // 2. Creating a new FieldManager (e.g., StoreEmbeddedFieldManager).
        // 3. Iterating through the embedded object's fields and storing them with prefixed property names
        //    on the owner Node.
        
        throw new NucleusUserException("Persistence of embedded fields is not yet supported: " + mmd.getFullFieldName());
    }
}
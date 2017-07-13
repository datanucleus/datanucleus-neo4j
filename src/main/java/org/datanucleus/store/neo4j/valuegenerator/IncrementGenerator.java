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
package org.datanucleus.store.neo4j.valuegenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.connection.ManagedConnection;
import org.datanucleus.store.valuegenerator.AbstractConnectedGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;
import org.datanucleus.util.Localiser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;

/**
 * Generator that uses a Node in Neo4j to store and allocate identity values.
 * Each class/field where "increment" is specified will have its own Node with current value property, and
 * all "increment" Nodes are in a special index "DN_INCREMENT_INDEX".
 */
public class IncrementGenerator extends AbstractConnectedGenerator<Long>
{
    /** The index containing all increment nodes. */
    protected static final String INCREMENT_INDEX = "DN_INCREMENT_INDEX";

    /** The name of the attribute that this class/field is indexed as. */
    protected static final String INCREMENT_NAME = "INCREMENT_NAME";

    /** The name of the property defining the current value for this increment generator. */
    protected static final String INCREMENT_VALUE_PROPERTY = "INCREMENT_VAL";

    /**
     * Constructor. Will receive the following properties (as a minimum) through this constructor.
     * <ul>
     * <li>class-name : Name of the class whose object is being inserted.</li>
     * <li>root-class-name : Name of the root class in this inheritance tree</li>
     * <li>field-name : Name of the field with the strategy (unless datastore identity field)</li>
     * <li>catalog-name : Catalog of the table (if specified)</li>
     * <li>schema-name : Schema of the table (if specified)</li>
     * <li>table-name : Name of the root table for this inheritance tree (containing the field).</li>
     * <li>column-name : Name of the column in the table (for the field)</li>
     * <li>sequence-name : Name of the sequence (if specified in MetaData as "sequence)</li>
     * </ul>
     * @param storeMgr StoreManager
     * @param name Symbolic name for the generator
     * @param props Properties controlling the behaviour of the generator (or null if not required).
     */
    public IncrementGenerator(StoreManager storeMgr, String name, Properties props)
    {
        super(storeMgr, name, props);
    }

    @Override
    protected ValueGenerationBlock<Long> reserveBlock(long size)
    {
        List<Long> oids = new ArrayList<Long>();
        try
        {
            ManagedConnection mconn = connectionProvider.retrieveConnection();
            GraphDatabaseService db = (GraphDatabaseService)mconn.getConnection();

            // Find the generator Node (if present)
            Node generatorNode = null;
            if (db.index().existsForNodes(INCREMENT_INDEX))
            {
                String cypherStr = "START n=node:" + INCREMENT_INDEX + "(" + INCREMENT_NAME + "=\"" + name + "\") RETURN n";
                Result queryResult = db.execute(cypherStr);
                while (queryResult.hasNext())
                {
                    Map<String, Object> map = queryResult.next();
                    generatorNode = (Node) map.get("n");
                    break;
                }
                queryResult.close();
            }

            if (generatorNode == null)
            {
                if (!storeMgr.getSchemaHandler().isAutoCreateTables())
                {
                    throw new NucleusUserException(Localiser.msg("040011", name));
                }

                // Create the Node
                generatorNode = db.createNode();
                generatorNode.setProperty(INCREMENT_VALUE_PROPERTY, Long.valueOf(0));
                db.index().forNodes(INCREMENT_INDEX).add(generatorNode, INCREMENT_NAME, name);
            }

            // Allocate "size" entries in "oids"
            long number = (Long)generatorNode.getProperty(INCREMENT_VALUE_PROPERTY);
            for (int i=0;i<size;i++)
            {
                number++;
                oids.add(number);
            }

            generatorNode.setProperty(INCREMENT_VALUE_PROPERTY, number);
        }
        finally
        {
            connectionProvider.releaseConnection();
        }

        return new ValueGenerationBlock<Long>(oids);
    }
}
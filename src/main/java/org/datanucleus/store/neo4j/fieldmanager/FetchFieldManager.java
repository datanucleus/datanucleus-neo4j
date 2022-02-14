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

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.PersistableObjectType;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.DNStateManager;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.datanucleus.store.neo4j.Neo4jUtils;
import org.datanucleus.store.query.QueryUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.MultiColumnConverter;
import org.datanucleus.store.types.converters.TypeConverter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Field Manager for retrieving values from Neo4j.
 */
public class FetchFieldManager extends AbstractFetchFieldManager
{
    protected Table table;

    protected PropertyContainer propObj;

    boolean embedded = false;

    public FetchFieldManager(DNStateManager sm, PropertyContainer node, Table table)
    {
        super(sm);
        this.table = table;
        this.propObj = node;
        if (ec.getOwnersForEmbeddedStateManager(sm) != null)
        {
            embedded = true;
        }
    }

    public FetchFieldManager(ExecutionContext ec, PropertyContainer node, AbstractClassMetaData cmd, Table table)
    {
        super(ec, cmd);
        this.table = table;
        this.propObj = node;
        if (node == null)
        {
            throw new NucleusException("Attempt to create FetchFieldManager for " + sm + " with null Neo4j Node!" +
                " Generate a testcase that reproduces this and raise an issue");
        }
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    private Object getPropertyForColumn(PropertyContainer propObj, String columnName)
    {
        return (propObj.hasProperty(columnName)) ? propObj.getProperty(columnName) : null;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        Boolean val = (Boolean)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? false : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        Byte val = (Byte)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        Character val = (Character)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        Double val = (Double)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        Float val = (Float)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        Integer val = (Integer)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        Long val = (Long)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        Short val = (Short)getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
        return (val == null) ? 0 : val;
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        return (String) getPropertyForColumn(propObj, getColumnMapping(fieldNumber).getColumn(0).getName());
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchObjectField(int)
     */
    @Override
    public Object fetchObjectField(int fieldNumber)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (mmd.getPersistenceModifier() != FieldPersistenceModifier.PERSISTENT)
        {
            return sm.provideField(fieldNumber);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE)
        {
            if (MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
            {
                // Embedded field
                if (RelationType.isRelationSingleValued(relationType))
                {
                    // Embedded PC object
                    AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                    if (embcmd == null)
                    {
                        throw new NucleusUserException("Field " + mmd.getFullFieldName() + " marked as embedded but no such metadata");
                    }

                    // TODO Cater for null (use embmd.getNullIndicatorColumn/Value)

                    // TODO Cater for inherited embedded objects (discriminator)

                    List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                    embMmds.add(mmd);
                    DNStateManager embSM = ec.getNucleusContext().getStateManagerFactory().newForEmbedded(ec, embcmd, sm, fieldNumber, PersistableObjectType.EMBEDDED_PC);
                    FieldManager ffm = new FetchEmbeddedFieldManager(embSM, propObj, embMmds, table);
                    embSM.replaceFields(embcmd.getAllMemberPositions(), ffm);
                    return embSM.getObject();
                }
                else if (RelationType.isRelationMultiValued(relationType))
                {
                    throw new NucleusUserException("Dont currently support embedded multivalued field : " + mmd.getFullFieldName());
                }
            }
        }

        return fetchNonEmbeddedObjectField(mmd, relationType, clr);
    }

    protected Object fetchNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        boolean optional = false;
        if (Optional.class.isAssignableFrom(mmd.getType()))
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }
            optional = true;
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (!(propObj instanceof Node))
            {
                throw new NucleusUserException("Object " + sm + " is mapped to a Relationship. Not yet supported");
            }

            Object value = processSingleValuedRelationForNode(mmd, relationType, clr, (Node)propObj);
            return optional ? (value!=null ? Optional.of(value) : Optional.empty()) : value;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (!(propObj instanceof Node))
            {
                // Any object mapped as a Relationship cannot have multi-value relations, only a source and target
                throw new NucleusUserException("Object " + sm + " is mapped to a Relationship but has field " + 
                    mmd.getFullFieldName() + " which is multi-valued. This is illegal");
            }

            return processMultiValuedRelationForNode(mmd, relationType, clr, (Node)propObj);
        }

        String propName = mapping.getColumn(0).getName(); // TODO Support multicol members
        if (!propObj.hasProperty(propName))
        {
            return optional ? Optional.empty() : null;
        }
        Object value = propObj.getProperty(propName);

        if (mmd.isSerialized())
        {
            if (value instanceof String)
            {
                TypeConverter<Serializable, String> conv = ec.getTypeManager().getTypeConverterForType(Serializable.class, String.class);
                return conv.toMemberType((String) value);
            }

            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a serialised value," +
                    " but we only support String serialisation and is " + value.getClass().getName());
        }

        Object returnValue = null;
        if (mapping.getTypeConverter() != null)
        {
            TypeConverter conv = mapping.getTypeConverter();
            if (mapping.getNumberOfColumns() > 1)
            {
                boolean isNull = true;
                Object valuesArr = null;
                Class[] colTypes = ((MultiColumnConverter)conv).getDatastoreColumnTypes();
                if (colTypes[0] == int.class)
                {
                    valuesArr = new int[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == long.class)
                {
                    valuesArr = new long[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == double.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == float.class)
                {
                    valuesArr = new double[mapping.getNumberOfColumns()];
                }
                else if (colTypes[0] == String.class)
                {
                    valuesArr = new String[mapping.getNumberOfColumns()];
                }
                // TODO Support other types
                else
                {
                    valuesArr = new Object[mapping.getNumberOfColumns()];
                }

                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    String colName = mapping.getColumn(i).getName();
                    if (propObj.hasProperty(colName))
                    {
                        isNull = false;
                        Array.set(valuesArr, i, propObj.getProperty(colName));
                    }
                    else
                    {
                        Array.set(valuesArr, i, null);
                    }
                }

                if (isNull)
                {
                    return null;
                }

                Object memberValue = conv.toMemberType(valuesArr);
                if (sm != null && memberValue != null)
                {
                    memberValue = SCOUtils.wrapSCOField(sm, fieldNumber, memberValue, true);
                }
                return memberValue;
            }

            String colName = mapping.getColumn(0).getName();
            if (!propObj.hasProperty(colName))
            {
                return null;
            }
            Object propVal = propObj.getProperty(colName);
            returnValue = conv.toMemberType(propVal);

            if (optional)
            {
                returnValue = (returnValue!=null) ? Optional.of(returnValue) : Optional.empty();
            }
            if (sm != null)
            {
                returnValue = SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), returnValue, true);
            }
            return returnValue;
        }

        Object fieldValue = Neo4jUtils.getFieldValueFromStored(ec, mmd, value, FieldRole.ROLE_FIELD);
        if (optional)
        {
            fieldValue = (fieldValue!=null) ? Optional.of(fieldValue) : Optional.empty();
        }
        return (sm!=null) ? SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), fieldValue, true) : fieldValue;
    }

    protected Object processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Node node)
    {
        RelationshipType type = DNRelationshipType.SINGLE_VALUED;
        if (relationType == RelationType.MANY_TO_ONE_BI)
        {
            type = DNRelationshipType.MULTI_VALUED;
        }

        AbstractClassMetaData relCmd = null;
        String propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME;
        String propNameValue = mmd.getName();
        if (mmd.getMappedBy() != null)
        {
            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
            propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER;
            relCmd = relMmds[0].getAbstractClassMetaData();
        }
        else if (relationType == RelationType.MANY_TO_ONE_BI)
        {
            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
            propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER;
            relCmd = relMmds[0].getAbstractClassMetaData();
        }
        else
        {
            boolean optional = (Optional.class.isAssignableFrom(mmd.getType()));
            Class memberType = optional ? clr.classForName(mmd.getCollection().getElementType()) : mmd.getType();
            relCmd = ec.getMetaDataManager().getMetaDataForClass(memberType, clr);
        }

        Iterable<Relationship> rels = node.getRelationships(type);
        if (rels != null)
        {
            Iterator<Relationship> relIter = rels.iterator();
            while (relIter.hasNext())
            {
                Relationship rel = relIter.next();
                String memberName = (String) rel.getProperty(propNameKey);
                if (memberName != null && memberName.equals(propNameValue))
                {
                    if (!RelationType.isBidirectional(relationType) && !rel.getStartNode().equals(node))
                    {
                        // Not a relation starting at this node so ignore
                        continue;
                    }

                    Node relNode = rel.getOtherNode(node);
                    return Neo4jUtils.getObjectForPropertyContainer(relNode, Neo4jUtils.getClassMetaDataForPropertyContainer(relNode, ec, relCmd), ec, false);
                }
            }
        }

        return null;
    }

    protected Object processMultiValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Node node)
    {
        if (mmd.hasCollection())
        {
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_COLLECTION_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has a collection of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            String propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME;
            if (relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() != null)
            {
                propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER;
            }
            Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.MULTI_VALUED);
            if (rels != null)
            {
                if (coll instanceof List)
                {
                    // Load the objects into array using the respective index positions, then into the collection
                    Map<Integer, Node> nodeByPos = new HashMap<Integer, Node>();
                    Iterator<Relationship> relIter = rels.iterator();
                    while (relIter.hasNext())
                    {
                        Relationship rel = relIter.next();
                        String relMemberName = (String) rel.getProperty(propNameKey);
                        if (relMemberName != null && relMemberName.equals(mmd.getName()))
                        {
                            int pos = (Integer)rel.getProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME);
                            Node elemNode = rel.getOtherNode(node);
                            nodeByPos.put(pos, elemNode);
                        }
                    }

                    Object[] array = new Object[nodeByPos.size()];
                    Iterator mapEntryIter = nodeByPos.entrySet().iterator();
                    while (mapEntryIter.hasNext())
                    {
                        Map.Entry entry = (Entry) mapEntryIter.next();
                        Integer pos = (Integer)entry.getKey();
                        Node elemNode = (Node)entry.getValue();
                        Object elemPC = Neo4jUtils.getObjectForPropertyContainer(elemNode,
                            Neo4jUtils.getClassMetaDataForPropertyContainer(elemNode, ec, elemCmd), ec, false);
                        array[pos] = elemPC;
                    }
                    for (int i=0;i<array.length;i++)
                    {
                        coll.add(array[i]);
                    }
                    array = null;

                    if (coll instanceof List && mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null && !mmd.getOrderMetaData().getOrdering().equals("#PK"))
                    {
                        // Reorder the collection as per the ordering clause
                        Collection newColl = QueryUtils.orderCandidates((List)coll, clr.classForName(mmd.getCollection().getElementType()), mmd.getOrderMetaData().getOrdering(), ec, clr);
                        if (newColl.getClass() != coll.getClass())
                        {
                            // Type has changed, so just reuse the input
                            coll.clear();
                            coll.addAll(newColl);
                        }
                    }
                }
                else
                {
                    Iterator<Relationship> relIter = rels.iterator();
                    while (relIter.hasNext())
                    {
                        Relationship rel = relIter.next();
                        String relMemberName = (String) rel.getProperty(propNameKey);
                        if (relMemberName != null && relMemberName.equals(mmd.getName()))
                        {
                            Node elemNode = rel.getOtherNode(node);
                            Object elemPC = Neo4jUtils.getObjectForPropertyContainer(elemNode, Neo4jUtils.getClassMetaDataForPropertyContainer(elemNode, ec, elemCmd), ec, false);
                            coll.add(elemPC);
                        }
                    }
                }
            }

            if (sm != null)
            {
                // Wrap if SCO
                return SCOUtils.wrapSCOField(sm, mmd.getAbsoluteFieldNumber(), coll, false);
            }
            return coll;
        }
        else if (mmd.hasArray())
        {
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr);
            if (elemCmd == null)
            {
                // Try any listed implementations
                String[] implNames = MetaDataUtils.getInstance().getImplementationNamesForReferenceField(mmd, 
                    FieldRole.ROLE_ARRAY_ELEMENT, clr, ec.getMetaDataManager());
                if (implNames != null && implNames.length == 1)
                {
                    elemCmd = ec.getMetaDataManager().getMetaDataForClass(implNames[0], clr);
                }
                if (elemCmd == null)
                {
                    throw new NucleusUserException("We do not currently support the field type of " + mmd.getFullFieldName() +
                        " which has an array of interdeterminate element type (e.g interface or Object element types)");
                }
            }

            Object array = null;
            int arraySize = 0;
            String propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME;
            if (relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() != null)
            {
                propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER;
            }
            Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.MULTI_VALUED);
            if (rels != null)
            {
                Iterator<Relationship> relIter = rels.iterator();
                while (relIter.hasNext())
                {
                    Relationship rel = relIter.next();
                    String relMemberName = (String) rel.getProperty(propNameKey);
                    if (relMemberName != null && relMemberName.equals(mmd.getName()))
                    {
                        arraySize++;
                    }
                }

                int i = 0;
                array = Array.newInstance(mmd.getType().getComponentType(), arraySize);

                relIter = rels.iterator();
                while (relIter.hasNext())
                {
                    Relationship rel = relIter.next();
                    String relMemberName = (String) rel.getProperty(propNameKey);
                    if (relMemberName != null && relMemberName.equals(mmd.getName()))
                    {
                        int position = i;
                        if (rel.hasProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME))
                        {
                            position = (Integer) rel.getProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME);
                        }
                        Node elemNode = rel.getOtherNode(node);
                        Object elemPC = Neo4jUtils.getObjectForPropertyContainer(elemNode, 
                            Neo4jUtils.getClassMetaDataForPropertyContainer(elemNode, ec, elemCmd), ec, false);
                        Array.set(array, position, elemPC);
                        i++;
                    }
                }
            }

            return array;
        }
        else if (mmd.hasMap())
        {
            Map map = null;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                map = (Map<Object, Object>) instanceType.getDeclaredConstructor().newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr);
            AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr);
            String propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME;
            if (relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() != null)
            {
                propNameKey = Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER;
            }
            if (!mmd.getMap().keyIsPersistent() && mmd.getMap().valueIsPersistent())
            {
                // Map<NonPC, PC> : Value stored as Node, and Relationship "owner - value" with key as property on Relationship
                Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.MULTI_VALUED);
                if (rels != null)
                {
                    Iterator<Relationship> relIter = rels.iterator();
                    while (relIter.hasNext())
                    {
                        Relationship rel = relIter.next();
                        String relMemberName = (String) rel.getProperty(propNameKey);
                        if (relMemberName != null && relMemberName.equals(mmd.getName()))
                        {
                            // Relationship for this field, so add to the Map
                            Node valNode = rel.getOtherNode(node);
                            Object val = Neo4jUtils.getObjectForPropertyContainer(valNode, 
                                Neo4jUtils.getClassMetaDataForPropertyContainer(valNode, ec, valCmd), ec, false);
                            Object key = null;
                            if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
                            {
                                // Key is field of value
                                DNStateManager valSM = ec.findStateManager(val);
                                key = valSM.provideField(valCmd.getAbsolutePositionOfMember(mmd.getKeyMetaData().getMappedBy()));
                            }
                            else
                            {
                                // Key is separate object so store as property on Relationship
                                key = Neo4jUtils.getFieldValueFromStored(ec, mmd, rel.getProperty(Neo4jStoreManager.RELATIONSHIP_MAP_KEY_VALUE), FieldRole.ROLE_MAP_KEY);
                            }
                            map.put(key,  val);
                        }
                    }
                }
                return map;
            }
            else if (mmd.getMap().keyIsPersistent() && !mmd.getMap().valueIsPersistent())
            {
                // Map<PC, NonPC>
                Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.MULTI_VALUED);
                if (rels != null)
                {
                    Iterator<Relationship> relIter = rels.iterator();
                    while (relIter.hasNext())
                    {
                        Relationship rel = relIter.next();
                        String relMemberName = (String) rel.getProperty(propNameKey);
                        if (relMemberName != null && relMemberName.equals(mmd.getName()))
                        {
                            // Relationship for this field, so add to the Map
                            Node keyNode = rel.getOtherNode(node);
                            Object key = Neo4jUtils.getObjectForPropertyContainer(keyNode, 
                                Neo4jUtils.getClassMetaDataForPropertyContainer(keyNode, ec, keyCmd), ec, false);
                            Object val = null;
                            if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() != null)
                            {
                                // Value is field of key
                                DNStateManager keySM = ec.findStateManager(key);
                                val = keySM.provideField(keyCmd.getAbsolutePositionOfMember(mmd.getValueMetaData().getMappedBy()));
                            }
                            else
                            {
                                // Value is separate object so store as property on Relationship
                                val = Neo4jUtils.getFieldValueFromStored(ec, mmd, rel.getProperty(Neo4jStoreManager.RELATIONSHIP_MAP_VAL_VALUE), FieldRole.ROLE_MAP_VALUE);
                            }
                            map.put(key,  val);
                        }
                    }
                }
                return map;
            }
            else
            {
                // Map<PC, PC> : not supported TODO Support this somehow
                throw new NucleusUserException("Don't currently support maps of persistable objects : " + mmd.getFullFieldName());
            }
        }
        return null;
    }
}
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.metadata.FieldPersistenceModifier;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.query.QueryUtils;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFetchFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.datanucleus.store.neo4j.Neo4jUtils;
import org.datanucleus.store.schema.naming.ColumnType;
import org.datanucleus.store.types.SCOUtils;
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
    protected PropertyContainer propObj;

    boolean embedded = false;

    /** Metadata for the owner field if this is embedded. */
    protected AbstractMemberMetaData ownerMmd = null;

    public FetchFieldManager(ObjectProvider op, PropertyContainer node)
    {
        super(op);
        this.propObj = node;
        if (op.getEmbeddedOwners() != null)
        {
            embedded = true;
        }
    }

    public FetchFieldManager(ExecutionContext ec, PropertyContainer node, AbstractClassMetaData cmd)
    {
        super(ec, cmd);
        this.propObj = node;
        if (node == null)
        {
            throw new NucleusException("Attempt to create FetchFieldManager for " + op + " with null Neo4j Node!" +
                " Generate a testcase that reproduces this and raise an issue");
        }
    }

    protected String getPropName(int fieldNumber)
    {
        return ec.getStoreManager().getNamingFactory().getColumnName(
            cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber), ColumnType.COLUMN);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchBooleanField(int)
     */
    @Override
    public boolean fetchBooleanField(int fieldNumber)
    {
        return (Boolean)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchByteField(int)
     */
    @Override
    public byte fetchByteField(int fieldNumber)
    {
        return (Byte)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchCharField(int)
     */
    @Override
    public char fetchCharField(int fieldNumber)
    {
        return (Character)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchDoubleField(int)
     */
    @Override
    public double fetchDoubleField(int fieldNumber)
    {
        return (Double)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchFloatField(int)
     */
    @Override
    public float fetchFloatField(int fieldNumber)
    {
        return (Float)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchIntField(int)
     */
    @Override
    public int fetchIntField(int fieldNumber)
    {
        return (Integer)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchLongField(int)
     */
    @Override
    public long fetchLongField(int fieldNumber)
    {
        return (Long)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchShortField(int)
     */
    @Override
    public short fetchShortField(int fieldNumber)
    {
        return (Short)propObj.getProperty(getPropName(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#fetchStringField(int)
     */
    @Override
    public String fetchStringField(int fieldNumber)
    {
        String propName = getPropName(fieldNumber);
        return propObj.hasProperty(propName) ? (String)propObj.getProperty(propName) : null;
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
            return op.provideField(fieldNumber);
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);

        // Determine if this field is stored embedded
        boolean embedded = false;
        if (mmd.isEmbedded() || mmd.getEmbeddedMetaData() != null)
        {
            // Field marked directly as embedded
            embedded = true;
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            // Field container contents marked directly as embedded
            if (mmd.hasCollection() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
            {
                // Embedded collection element
                embedded = true;
            }
            else if (mmd.hasArray() && mmd.getElementMetaData() != null && mmd.getElementMetaData().getEmbeddedMetaData() != null)
            {
                // Embedded collection element
                embedded = true;
            }
            else if (mmd.hasMap() && 
                    ((mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getEmbeddedMetaData() != null) || 
                    (mmd.getValueMetaData() != null && mmd.getValueMetaData().getEmbeddedMetaData() != null)))
            {
                // Embedded map key/value
                embedded = true;
            }
        }
        if (!embedded && ownerMmd != null)
        {
            // Check for any nested embedded information (like can be specified with JDO)
            if (RelationType.isRelationSingleValued(relationType))
            {
                if (ownerMmd.hasCollection())
                {
                    // This is a field of the element of the collection, so check for any metadata spec for it
                    EmbeddedMetaData embmd = ownerMmd.getElementMetaData().getEmbeddedMetaData();
                    if (embmd != null)
                    {
                        AbstractMemberMetaData[] embMmds = embmd.getMemberMetaData();
                        if (embMmds != null)
                        {
                            for (AbstractMemberMetaData embMmd : embMmds)
                            {
                                if (embMmd.getName().equals(mmd.getName()))
                                {
                                    if (embMmd.isEmbedded() || embMmd.getEmbeddedMetaData() != null)
                                    {
                                        // Embedded Field is marked in nested embedded definition as embedded
                                        embedded = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                else if (ownerMmd.getEmbeddedMetaData() != null)
                {
                    // This is a field of an embedded persistable object, so check for any metadata spec for it
                    AbstractMemberMetaData[] embMmds = ownerMmd.getEmbeddedMetaData().getMemberMetaData();
                    if (embMmds != null)
                    {
                        for (AbstractMemberMetaData embMmd : embMmds)
                        {
                            if (embMmd.getName().equals(mmd.getName()))
                            {
                                // Embedded Field is marked in nested embedded definition as embedded
                                embedded = true;
                                break;
                            }
                        }
                    }                    
                }
            }
        }

        if (embedded)
        {
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object
                // TODO Detect null embedded object
                if (ownerMmd != null)
                {
                    // Detect bidirectional relation so we know when to stop embedding
                    if (RelationType.isBidirectional(relationType))
                    {
                        if ((ownerMmd.getMappedBy() != null && mmd.getName().equals(ownerMmd.getMappedBy())) ||
                            (mmd.getMappedBy() != null && ownerMmd.getName().equals(mmd.getMappedBy())))
                        {
                            // Other side of owner bidirectional, so return the owner
                            ObjectProvider[] ownerOps = op.getEmbeddedOwners();
                            return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
                        }
                    }
                    else
                    {
                        // mapped-by not set but could have owner-field
                        if (ownerMmd.getEmbeddedMetaData() != null &&
                            ownerMmd.getEmbeddedMetaData().getOwnerMember() != null &&
                            ownerMmd.getEmbeddedMetaData().getOwnerMember().equals(mmd.getName()))
                        {
                            // This is the owner-field linking back to the owning object so return the owner
                            ObjectProvider[] ownerOps = op.getEmbeddedOwners();
                            return (ownerOps != null && ownerOps.length > 0 ? ownerOps[0].getObject() : null);
                        }
                    }
                }

                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
                if (embcmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                        " marked as embedded but no such metadata");
                }

                // Extract the owner member metadata for this embedded object
                AbstractMemberMetaData embMmd = mmd;
                if (ownerMmd != null)
                {
                    // Nested, so use from the embeddedMetaData
                    embMmd = ownerMmd.getEmbeddedMetaData().getMemberMetaData()[fieldNumber];
                }

                // TODO Cater for inherited embedded objects (discriminator)

                ObjectProvider embOP = ec.newObjectProviderForEmbedded(embcmd, op, fieldNumber);
                FieldManager ffm = new FetchEmbeddedFieldManager(embOP, propObj, embMmd);
                embOP.replaceFields(embcmd.getAllMemberPositions(), ffm);
                return embOP.getObject();
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                throw new NucleusUserException("Dont currently support embedded multivalued field : " + mmd.getFullFieldName());
            }
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (!(propObj instanceof Node))
            {
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship. Not yet supported");
            }

            Node node = (Node)propObj;
            return processSingleValuedRelationForNode(mmd, relationType, ec, clr, node);
        }
        else if (RelationType.isRelationMultiValued(relationType))
        {
            if (!(propObj instanceof Node))
            {
                // Any object mapped as a Relationship cannot have multi-value relations, only a source and target
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship but has field " + 
                    mmd.getFullFieldName() + " which is multi-valued. This is illegal");
            }

            Node node = (Node)propObj;
            return processMultiValuedRelationForNode(mmd, relationType, ec, clr, node);
        }

        String fieldName = getPropName(fieldNumber);
        if (!propObj.hasProperty(fieldName))
        {
            return null;
        }
        Object value = propObj.getProperty(fieldName);

        if (mmd.isSerialized())
        {
            if (value instanceof String)
            {
                TypeConverter<Serializable, String> conv = ec.getTypeManager().getTypeConverterForType(Serializable.class, String.class);
                return conv.toMemberType((String) value);
            }
            else
            {
                throw new NucleusUserException("Field " + mmd.getFullFieldName() + " has a serialised value," +
                    " but we only support String serialisation and is " + value.getClass().getName());
            }
        }

        if (mmd.getTypeConverterName() != null)
        {
            // User-defined type converter
            TypeConverter conv = ec.getNucleusContext().getTypeManager().getTypeConverterForName(mmd.getTypeConverterName());
            return conv.toMemberType(value);
        }

        Object fieldValue = Neo4jUtils.getFieldValueFromStored(ec, mmd, value, FieldRole.ROLE_FIELD);
        if (op != null)
        {
            // Wrap if SCO
            return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), fieldValue, false, false, true);
        }
        return fieldValue;
    }

    protected Object processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType,
            ExecutionContext ec, ClassLoaderResolver clr, Node node)
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
            relCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
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
                    Node relNode = rel.getOtherNode(node);
                    return Neo4jUtils.getObjectForPropertyContainer(relNode,
                        Neo4jUtils.getClassMetaDataForPropertyContainer(relNode, ec, relCmd), ec, false);
                }
            }
        }

        return null;
    }

    protected Object processMultiValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType,
            ExecutionContext ec, ClassLoaderResolver clr, Node node)
    {
        if (mmd.hasCollection())
        {
            Collection<Object> coll;
            try
            {
                Class instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData elemCmd = mmd.getCollection().getElementClassMetaData(clr, ec.getMetaDataManager());
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

                    if (mmd.getOrderMetaData() != null && mmd.getOrderMetaData().getOrdering() != null &&
                        !mmd.getOrderMetaData().getOrdering().equals("#PK"))
                    {
                        // Reorder the collection as per the ordering clause (DN 3.0.10+)
                        coll = QueryUtils.orderCandidates((List)coll, mmd.getType(), mmd.getOrderMetaData().getOrdering(), ec, clr);
                    }
                    array = null;
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
                            Object elemPC = Neo4jUtils.getObjectForPropertyContainer(elemNode, 
                                Neo4jUtils.getClassMetaDataForPropertyContainer(elemNode, ec, elemCmd), ec, false);
                            coll.add(elemPC);
                        }
                    }
                }
            }

            if (op != null)
            {
                // Wrap if SCO
                return op.wrapSCOField(mmd.getAbsoluteFieldNumber(), coll, false, false, false);
            }
            return coll;
        }
        else if (mmd.hasArray())
        {
            AbstractClassMetaData elemCmd = mmd.getArray().getElementClassMetaData(clr, ec.getMetaDataManager());
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
                map = (Map<Object, Object>) instanceType.newInstance();
            }
            catch (Exception e)
            {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }

            AbstractClassMetaData keyCmd = mmd.getMap().getKeyClassMetaData(clr, ec.getMetaDataManager());
            AbstractClassMetaData valCmd = mmd.getMap().getValueClassMetaData(clr, ec.getMetaDataManager());
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
                                ObjectProvider valOP = ec.findObjectProvider(val);
                                key = valOP.provideField(valCmd.getAbsolutePositionOfMember(mmd.getKeyMetaData().getMappedBy()));
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
                                ObjectProvider keyOP = ec.findObjectProvider(key);
                                val = keyOP.provideField(keyCmd.getAbsolutePositionOfMember(mmd.getValueMetaData().getMappedBy()));
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
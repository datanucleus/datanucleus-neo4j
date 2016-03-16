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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusUserException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.metadata.MetaDataUtils;
import org.datanucleus.metadata.RelationType;
import org.datanucleus.state.ObjectProvider;
import org.datanucleus.store.exceptions.ReachableObjectNotCascadedException;
import org.datanucleus.store.fieldmanager.AbstractStoreFieldManager;
import org.datanucleus.store.fieldmanager.FieldManager;
import org.datanucleus.store.neo4j.Neo4jStoreManager;
import org.datanucleus.store.neo4j.Neo4jUtils;
import org.datanucleus.store.schema.table.MemberColumnMapping;
import org.datanucleus.store.schema.table.Table;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;
import org.datanucleus.util.Localiser;
import org.datanucleus.util.NucleusLogger;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

/**
 * Field Manager for putting values from a POJO into a Neo4j Node.
 */
public class StoreFieldManager extends AbstractStoreFieldManager
{
    protected Table table;

    /** Node/Relationship that we are populating with properties representing the fields of the POJO. */
    protected PropertyContainer propObj;

    public StoreFieldManager(ObjectProvider op, PropertyContainer propObj, boolean insert, Table table)
    {
        super(op, insert);
        this.table = table;
        this.propObj = propObj;
    }

    public StoreFieldManager(ExecutionContext ec, AbstractClassMetaData cmd, PropertyContainer propObj, boolean insert, Table table)
    {
        super(ec, cmd, insert);
        this.table = table;
        this.propObj = propObj;
    }

    protected MemberColumnMapping getColumnMapping(int fieldNumber)
    {
        return table.getMemberColumnMappingForMember(cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber));
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeBooleanField(int, boolean)
     */
    @Override
    public void storeBooleanField(int fieldNumber, boolean value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeByteField(int, byte)
     */
    @Override
    public void storeByteField(int fieldNumber, byte value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeCharField(int, char)
     */
    @Override
    public void storeCharField(int fieldNumber, char value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeDoubleField(int, double)
     */
    @Override
    public void storeDoubleField(int fieldNumber, double value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeFloatField(int, float)
     */
    @Override
    public void storeFloatField(int fieldNumber, float value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeIntField(int, int)
     */
    @Override
    public void storeIntField(int fieldNumber, int value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeLongField(int, long)
     */
    @Override
    public void storeLongField(int fieldNumber, long value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeShortField(int, short)
     */
    @Override
    public void storeShortField(int fieldNumber, short value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        propObj.setProperty(getColumnMapping(fieldNumber).getColumn(0).getName(), value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeStringField(int, java.lang.String)
     */
    @Override
    public void storeStringField(int fieldNumber, String value)
    {
        if (!isStorable(fieldNumber))
        {
            return;
        }
        String propName = getColumnMapping(fieldNumber).getColumn(0).getName();
        if (value == null)
        {
            if (!insert)
            {
                propObj.removeProperty(propName);
            }
            return;
        }
        propObj.setProperty(propName, value);
    }

    /* (non-Javadoc)
     * @see org.datanucleus.store.fieldmanager.AbstractFieldManager#storeObjectField(int, java.lang.Object)
     */
    @Override
    public void storeObjectField(int fieldNumber, Object value)
    {
        AbstractMemberMetaData mmd = cmd.getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);
        if (!isStorable(mmd))
        {
            return;
        }

        ClassLoaderResolver clr = ec.getClassLoaderResolver();
        RelationType relationType = mmd.getRelationType(clr);
        if (relationType != RelationType.NONE && MetaDataUtils.getInstance().isMemberEmbedded(ec.getMetaDataManager(), clr, mmd, relationType, null))
        {
            // Embedded Field
            if (RelationType.isRelationSingleValued(relationType))
            {
                // Embedded PC object
                if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
                {
                    if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
                    {
                        // Related PC object not persistent, but cant do cascade-persist so throw exception
                        if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                        {
                            NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                        }
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
                    }
                }

                // TODO Cater for nulled embedded object on update

                List<AbstractMemberMetaData> embMmds = new ArrayList<AbstractMemberMetaData>();
                embMmds.add(mmd);

                if (value == null)
                {
                    AbstractClassMetaData embCmd = ec.getMetaDataManager().getMetaDataForClass(mmd.getType(), clr);
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

                AbstractClassMetaData embcmd = ec.getMetaDataManager().getMetaDataForClass(value.getClass(), clr);
                if (embcmd == null)
                {
                    throw new NucleusUserException("Field " + mmd.getFullFieldName() +
                        " specified as embedded but metadata not found for the class of type " + mmd.getTypeName());
                }

                ObjectProvider embOP = ec.findObjectProviderForEmbedded(value, op, mmd);
                // TODO Cater for inherited embedded objects (discriminator)

                FieldManager ffm = new StoreEmbeddedFieldManager(embOP, propObj, insert, embMmds, table);
                embOP.provideFields(embcmd.getAllMemberPositions(), ffm);
                return;
            }
            else if (RelationType.isRelationMultiValued(relationType))
            {
                // TODO Support embedded collections, arrays, maps?
                throw new NucleusUserException("Don't currently support embedded field : " + mmd.getFullFieldName());
            }
        }

        storeNonEmbeddedObjectField(mmd,relationType, clr, value);
    }

    protected void storeNonEmbeddedObjectField(AbstractMemberMetaData mmd, RelationType relationType, ClassLoaderResolver clr, Object value)
    {
        int fieldNumber = mmd.getAbsoluteFieldNumber();
        ExecutionContext ec = op.getExecutionContext();
        MemberColumnMapping mapping = getColumnMapping(fieldNumber);

        boolean optional = false;
        if (value instanceof Optional)
        {
            if (relationType != RelationType.NONE)
            {
                relationType = RelationType.ONE_TO_ONE_UNI;
            }

            optional = true;
            Optional opt = (Optional)value;
            if (opt.isPresent())
            {
                value = opt.get();
            }
            else
            {
                value = null;
            }
        }

        if (value == null)
        {
            if (insert)
            {
                // Don't store the property when null
            }
            else
            {
                // Has been set to null so remove the property
                for (int i=0;i<mapping.getNumberOfColumns();i++)
                {
                    String colName = mapping.getColumn(i).getName();
                    if (propObj.hasProperty(colName))
                    {
                        propObj.removeProperty(colName);
                    }
                }
            }
            return;
        }

        if (mmd.isSerialized())
        {
            if (value instanceof Serializable)
            {
                TypeConverter<Serializable, String> conv = ec.getTypeManager().getTypeConverterForType(Serializable.class, String.class);
                String strValue = conv.toDatastoreType((Serializable) value);
                propObj.setProperty(mapping.getColumn(0).getName(), strValue);
                return;
            }

            throw new NucleusUserException("Field " + mmd.getFullFieldName() + " is marked as serialised, but value is not Serializable");
        }

        if (RelationType.isRelationSingleValued(relationType))
        {
            if (!(propObj instanceof Node))
            {
                // TODO Work out if this is the source or the target
                throw new NucleusUserException("Object " + op + " is mapped to a Relationship. Not yet supported");
            }

            Node node = (Node)propObj;
            processSingleValuedRelationForNode(mmd, relationType, value, ec, clr, node);
            return;
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
            processMultiValuedRelationForNode(mmd, relationType, value, ec, clr, node);
        }
        else
        {
            if (mapping.getTypeConverter() != null)
            {
                // Persist using the provided converter
                Object datastoreValue = mapping.getTypeConverter().toDatastoreType(value);
                if (mapping.getNumberOfColumns() > 1)
                {
                    for (int i=0;i<mapping.getNumberOfColumns();i++)
                    {
                        // TODO Persist as the correct column type since the typeConverter type may not be directly persistable
                        Object colValue = Array.get(datastoreValue, i);
                        propObj.setProperty(mapping.getColumn(i).getName(), colValue);
                    }
                }
                else
                {
                    propObj.setProperty(mapping.getColumn(0).getName(), datastoreValue);
                }
            }
            else
            {
                Object storedValue = Neo4jUtils.getStoredValueForField(ec, mmd, value, FieldRole.ROLE_FIELD);
                if (storedValue != null)
                {
                    // Neo4j doesn't allow null values
                    propObj.setProperty(mapping.getColumn(0).getName(), storedValue);
                }
            }
        }

        if (optional)
        {
            value = Optional.of(value);
        }
        SCOUtils.wrapSCOField(op, fieldNumber, value, true);
    }

    protected void processSingleValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, Object value,
            ExecutionContext ec, ClassLoaderResolver clr, Node node)
    {
        if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
        {
            if (!ec.getApiAdapter().isDetached(value) && !ec.getApiAdapter().isPersistent(value))
            {
                // Related PC object not persistent, but cant do cascade-persist so throw exception
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                }
                throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), value);
            }
        }

        // 1-1/N-1 Make sure it is persisted and form the relation
        Object valuePC = (value != null ? ec.persistObjectInternal(value, null, -1, -1) : null);
        ObjectProvider relatedOP = (value != null ? ec.findObjectProvider(valuePC) : null);

        if (relationType != RelationType.MANY_TO_ONE_BI && mmd.getMappedBy() == null)
        {
            // Only have a Relationship if this side owns the relation
            Node relatedNode = (Node)
                (value != null ? Neo4jUtils.getPropertyContainerForObjectProvider(propObj.getGraphDatabase(), relatedOP) : null);

            boolean hasRelation = false;
            if (!insert)
            {
                // Check for old value and remove Relationship if to a different Node
                Iterable<Relationship> rels = node.getRelationships(DNRelationshipType.SINGLE_VALUED);
                Iterator<Relationship> relIter = rels.iterator();
                while (relIter.hasNext())
                {
                    Relationship rel = relIter.next();
                    if (rel.getProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME).equals(mmd.getName()))
                    {
                        // Check if existing relationship for this field is to the same node
                        Node currentNode = rel.getOtherNode(node);
                        if (currentNode.equals(relatedNode))
                        {
                            hasRelation = true;
                            break;
                        }

                        // Remove old Relationship TODO Cascade delete?
                        rel.delete();
                    }
                }
            }

            if (!hasRelation && relatedNode != null)
            {
                // Add the new Relationship
                Relationship rel = node.createRelationshipTo(relatedNode, DNRelationshipType.SINGLE_VALUED);
                rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                if (RelationType.isBidirectional(relationType))
                {
                    AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                }
            }
        }
    }

    protected void processMultiValuedRelationForNode(AbstractMemberMetaData mmd, RelationType relationType, Object value,
            ExecutionContext ec, ClassLoaderResolver clr, Node node)
    {
        if (mmd.hasCollection())
        {
            Collection coll = (Collection)value;
            if ((insert && !mmd.isCascadePersist()) || (!insert && !mmd.isCascadeUpdate()))
            {
                // Field doesnt support cascade-persist so no reachability
                if (NucleusLogger.PERSISTENCE.isDebugEnabled())
                {
                    NucleusLogger.PERSISTENCE.debug(Localiser.msg("007006", mmd.getFullFieldName()));
                }

                // Check for any persistable elements that aren't persistent
                for (Object element : coll)
                {
                    if (!ec.getApiAdapter().isDetached(element) && !ec.getApiAdapter().isPersistent(element))
                    {
                        // Element is not persistent so throw exception
                        throw new ReachableObjectNotCascadedException(mmd.getFullFieldName(), element);
                    }
                }
            }

            List<Node> relNodes = new ArrayList<Node>();
            if (value != null)
            {
                // Reachability : Persist any objects that are not yet persistent, gathering Node objects
                Iterator collIter = coll.iterator();
                while (collIter.hasNext())
                {
                    if (mmd.getCollection().isSerializedElement())
                    {
                        throw new NucleusUserException("Don't currently support serialised collection elements at " + mmd.getFullFieldName());
                    }

                    Object element = collIter.next();
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, null, -1, -1);
                        ObjectProvider relatedOP = ec.findObjectProvider(elementPC);
                        Node relatedNode = (Node)Neo4jUtils.getPropertyContainerForObjectProvider(propObj.getGraphDatabase(), relatedOP);
                        relNodes.add(relatedNode);
                    }
                    else
                    {
                        throw new NucleusUserException("Dont currently support having null elements in collections : " + mmd.getFullFieldName());
                    }
                }
            }

            if (relationType != RelationType.ONE_TO_MANY_BI && relationType != RelationType.ONE_TO_MANY_UNI &&
                !(relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() == null))
            {
                // We only store relations when we are the owner
                return;
            }

            if (insert)
            {
                // Insert of the collection, so create Relationship for each
                int index = 0;
                for (Node newNode : relNodes)
                {
                    Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                    if (coll instanceof List)
                    {
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, index);
                        index++;
                    }
                    if (RelationType.isBidirectional(relationType))
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                    }
                }
            }
            else
            {
                // Update of the collection so remove existing Relationship and create new
                // TODO Handle better detecting which are still present and which new/updated
                deleteRelationshipsForMultivaluedMember(node, mmd);

                int index = 0;
                for (Node newNode : relNodes)
                {
                    Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                    if (coll instanceof List)
                    {
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, index);
                        index++;
                    }
                    if (RelationType.isBidirectional(relationType))
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                    }
                }
            }
        }
        else if (mmd.hasArray())
        {
            List<Node> relNodes = new ArrayList<Node>();

            // Reachability : Persist any objects that are not yet persistent, gathering Node objects
            if (value != null)
            {
                for (int i=0;i<Array.getLength(value);i++)
                {
                    if (mmd.getArray().isSerializedElement())
                    {
                        throw new NucleusUserException("Don't currently support serialised array elements at " + mmd.getFullFieldName());
                    }
                    Object element = Array.get(value, i);
                    if (element != null)
                    {
                        Object elementPC = ec.persistObjectInternal(element, null, -1, -1);
                        ObjectProvider relatedOP = ec.findObjectProvider(elementPC);
                        Node relatedNode = (Node)Neo4jUtils.getPropertyContainerForObjectProvider(propObj.getGraphDatabase(), relatedOP);
                        relNodes.add(relatedNode);
                    }
                    else
                    {
                        throw new NucleusUserException("Dont currently support having null elements in arrays : " + mmd.getFullFieldName());
                    }
                }
            }

            if (relationType != RelationType.ONE_TO_MANY_BI && relationType != RelationType.ONE_TO_MANY_UNI &&
                !(relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() == null))
            {
                // We only store relations when we are the owner
                return;
            }

            if (insert)
            {
                // Insert of the array, so create Relationship for each
                int index = 0;
                for (Node newNode : relNodes)
                {
                    Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, index);
                    if (RelationType.isBidirectional(relationType))
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                    }
                    index++;
                }
            }
            else
            {
                // Update of the array so remove existing Relationship and create new
                // TODO Handle better detecting which are still present and which new/updated
                deleteRelationshipsForMultivaluedMember(node, mmd);

                int index = 0;
                for (Node newNode : relNodes)
                {
                    Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                    rel.setProperty(Neo4jStoreManager.RELATIONSHIP_INDEX_NAME, index);
                    if (RelationType.isBidirectional(relationType))
                    {
                        AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                    }
                    index++;
                }
            }
        }
        else if (mmd.hasMap())
        {
            Map map = (Map)value;
            if (!mmd.getMap().keyIsPersistent() && mmd.getMap().valueIsPersistent())
            {
                List<Node> relNodes = new ArrayList<Node>();
                List relKeyValues = new ArrayList();
                if (map != null)
                {
                    // Reachability : Persist any objects that are not yet persistent, gathering Node objects
                    Iterator<Map.Entry> mapEntryIter = map.entrySet().iterator();
                    while (mapEntryIter.hasNext())
                    {
                        if (mmd.getMap().isSerializedValue())
                        {
                            throw new NucleusUserException("Don't currently support serialised map values at " + mmd.getFullFieldName());
                        }

                        Map.Entry entry = mapEntryIter.next();
                        Object key = entry.getKey();
                        Object val = entry.getValue();
                        if (val != null)
                        {
                            Object valPC = ec.persistObjectInternal(val, null, -1, -1);
                            ObjectProvider relatedOP = ec.findObjectProvider(valPC);
                            Node relatedNode = (Node)Neo4jUtils.getPropertyContainerForObjectProvider(propObj.getGraphDatabase(), relatedOP);
                            relNodes.add(relatedNode);
                            relKeyValues.add(Neo4jUtils.getStoredValueForField(ec, mmd, key, FieldRole.ROLE_MAP_KEY));
                        }
                        else
                        {
                            throw new NucleusUserException("Dont currently support having null values in maps : " + mmd.getFullFieldName());
                        }
                    }
                }

                if (relationType != RelationType.ONE_TO_MANY_BI && relationType != RelationType.ONE_TO_MANY_UNI &&
                    !(relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() == null))
                {
                    // We only store relations when we are the owner
                    return;
                }

                if (insert)
                {
                    // Insert of the map, so create Relationship owner-value (with key as property in some cases)
                    Iterator relKeyIter = relKeyValues.iterator();
                    for (Node newNode : relNodes)
                    {
                        Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                        if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
                        {
                            // Do nothing, key stored in field in value
                        }
                        else
                        {
                            // Store key in property on Relationship
                            Object relKeyVal = relKeyIter.next();
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_MAP_KEY_VALUE, relKeyVal);
                        }
                        if (RelationType.isBidirectional(relationType))
                        {
                            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                        }
                    }
                }
                else
                {
                    // Update of the map so remove existing Relationships and create new
                    // TODO Handle better detecting which are still present and which new/updated
                    deleteRelationshipsForMultivaluedMember(node, mmd);

                    Iterator relKeyIter = relKeyValues.iterator();
                    for (Node newNode : relNodes)
                    {
                        Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                        if (mmd.getKeyMetaData() != null && mmd.getKeyMetaData().getMappedBy() != null)
                        {
                            // Do nothing, key stored in field in value
                        }
                        else
                        {
                            // Store key in property on Relationship
                            Object relKeyVal = relKeyIter.next();
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_MAP_KEY_VALUE, relKeyVal);
                        }
                        if (RelationType.isBidirectional(relationType))
                        {
                            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                        }
                    }
                }
            }
            else if (mmd.getMap().keyIsPersistent() && !mmd.getMap().valueIsPersistent())
            {
                List<Node> relNodes = new ArrayList<Node>();
                List relValValues = new ArrayList();
                if (map != null)
                {
                    // Reachability : Persist any objects that are not yet persistent, gathering Node objects
                    Iterator<Map.Entry> mapEntryIter = map.entrySet().iterator();
                    while (mapEntryIter.hasNext())
                    {
                        if (mmd.getMap().isSerializedKey())
                        {
                            throw new NucleusUserException("Don't currently support serialised map keys at " + mmd.getFullFieldName());
                        }

                        Map.Entry entry = mapEntryIter.next();
                        Object key = entry.getKey();
                        Object val = entry.getValue();
                        if (val != null)
                        {
                            Object keyPC = ec.persistObjectInternal(key, null, -1, -1);
                            ObjectProvider relatedOP = ec.findObjectProvider(keyPC);
                            Node relatedNode = (Node)Neo4jUtils.getPropertyContainerForObjectProvider(propObj.getGraphDatabase(), relatedOP);
                            relNodes.add(relatedNode);
                            relValValues.add(Neo4jUtils.getStoredValueForField(ec, mmd, val, FieldRole.ROLE_MAP_VALUE));
                        }
                        else
                        {
                            throw new NucleusUserException("Dont currently support having null keys in maps : " + mmd.getFullFieldName());
                        }
                    }
                }

                if (relationType != RelationType.ONE_TO_MANY_BI && relationType != RelationType.ONE_TO_MANY_UNI &&
                    !(relationType == RelationType.MANY_TO_MANY_BI && mmd.getMappedBy() == null))
                {
                    // We only store relations when we are the owner
                    return;
                }

                if (insert)
                {
                    // Insert of the map, so create Relationship owner-key (with value as property in some cases)
                    Iterator relValIter = relValValues.iterator();
                    for (Node newNode : relNodes)
                    {
                        Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                        if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() != null)
                        {
                            // Do nothing, value stored in field in key
                        }
                        else
                        {
                            // Store value in property on Relationship
                            Object relValValue = relValIter.next();
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_MAP_VAL_VALUE, relValValue);
                        }
                        if (RelationType.isBidirectional(relationType))
                        {
                            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                        }
                    }
                }
                else
                {
                    // Update of the map so remove existing Relationships and create new
                    // TODO Handle better detecting which are still present and which new/updated
                    deleteRelationshipsForMultivaluedMember(node, mmd);

                    Iterator relValIter = relValValues.iterator();
                    for (Node newNode : relNodes)
                    {
                        Relationship rel = node.createRelationshipTo(newNode, DNRelationshipType.MULTI_VALUED);
                        rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME, mmd.getName());
                        if (mmd.getValueMetaData() != null && mmd.getValueMetaData().getMappedBy() != null)
                        {
                            // Do nothing, value stored in field in key
                        }
                        else
                        {
                            // Store value in property on Relationship
                            Object relValValue = relValIter.next();
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_MAP_VAL_VALUE, relValValue);
                        }
                        if (RelationType.isBidirectional(relationType))
                        {
                            AbstractMemberMetaData[] relMmds = mmd.getRelatedMemberMetaData(clr);
                            rel.setProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME_NONOWNER, relMmds[0].getName());
                        }
                    }
                }
            }
            else
            {
                // TODO Persist map<PC,PC>
                throw new NucleusUserException("Don't currently support maps of persistable objects : " + mmd.getFullFieldName());
            }
        }
    }

    /**
     * Convenience method that finds all relationships from the provided owner node and deletes all that
     * are for the specified field.
     * @param ownerNode The owner Node
     * @param mmd Metadata for the member that we are removing relationships for
     */
    private void deleteRelationshipsForMultivaluedMember(Node ownerNode, AbstractMemberMetaData mmd)
    {
        Iterable<Relationship> rels = ownerNode.getRelationships(DNRelationshipType.MULTI_VALUED);
        Iterator<Relationship> relIter = rels.iterator();
        while (relIter.hasNext())
        {
            Relationship rel = relIter.next();
            if (rel.getProperty(Neo4jStoreManager.RELATIONSHIP_FIELD_NAME).equals(mmd.getName()))
            {
                rel.delete();
            }
        }
    }
}
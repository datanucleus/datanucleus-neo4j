/**********************************************************************
Copyright (c) 2024 Andy Jefferson and others. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUTHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors:
    ...
**********************************************************************/
package org.datanucleus.store.neo4j;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Optional;

import org.datanucleus.ExecutionContext;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.FieldRole;
import org.datanucleus.store.types.SCOUtils;
import org.datanucleus.store.types.converters.EnumConversionHelper;
import org.datanucleus.store.types.converters.TypeConverter;
import org.datanucleus.util.ClassUtils;

/**
 * Manages the conversion of Java types to and from Neo4j-compatible property values.
 */
public final class Neo4jPropertyManager {

    private Neo4jPropertyManager() {
        // Private constructor to prevent instantiation
    }

    /**
     * Converts a Java field value into a type that can be stored as a Neo4j property.
     */
    @SuppressWarnings("unchecked")
    public static Object getStoredValueForField(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole) {
        if (value == null) {
            return null;
        }

        boolean optional = (mmd != null && Optional.class.isAssignableFrom(mmd.getType()));
        Class<?> type = value.getClass();

        if (mmd != null) {
            if (optional) {
                type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            } else {
                if (fieldRole == FieldRole.ROLE_COLLECTION_ELEMENT) {
                    type = ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
                } else if (fieldRole == FieldRole.ROLE_ARRAY_ELEMENT) {
                    type = ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
                } else if (fieldRole == FieldRole.ROLE_MAP_KEY) {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
                } else if (fieldRole == FieldRole.ROLE_MAP_VALUE) {
                    type = ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
                } else {
                    type = mmd.getType();
                }
            }
        }

        if (mmd != null && mmd.hasCollection() && !optional && fieldRole == FieldRole.ROLE_FIELD) {
            Collection<?> rawColl = (Collection<?>) value;
            if (rawColl.isEmpty()) {
                return null;
            }
            Object[] objArray = new Object[rawColl.size()];
            int i = 0;
            for (Object elem : rawColl) {
                objArray[i++] = getStoredValueForField(ec, mmd, elem, FieldRole.ROLE_COLLECTION_ELEMENT);
            }
            return convertArrayToStorableArray(objArray, mmd);
        } else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD) {
            if (Array.getLength(value) == 0) {
                return null;
            }
            if (type.getComponentType().isPrimitive() || String.class.isAssignableFrom(type.getComponentType())) {
                return value; // Natively supported array types
            }
            Object[] objArray = new Object[Array.getLength(value)];
            for (int i = 0; i < objArray.length; i++) {
                objArray[i] = getStoredValueForField(ec, mmd, Array.get(value, i), FieldRole.ROLE_ARRAY_ELEMENT);
            }
            return convertArrayToStorableArray(objArray, mmd);
        }

        if (isNativelySupported(type)) {
            return value;
        }

        if (Enum.class.isAssignableFrom(type)) {
            return EnumConversionHelper.getStoredValueFromEnum(mmd, fieldRole, (Enum<?>) value);
        }

        @SuppressWarnings("rawtypes")
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(type, String.class);
        if (strConv != null) {
            return strConv.toDatastoreType(value);
        }

        @SuppressWarnings("rawtypes")
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(type, Long.class);
        if (longConv != null) {
            return longConv.toDatastoreType(value);
        }

        // Fallback for other types that might not have a direct converter
        return value.toString();
    }

    /**
     * Converts a stored Neo4j property value back into its Java field type.
     */
    @SuppressWarnings("rawtypes")
    public static Object getFieldValueFromStored(ExecutionContext ec, AbstractMemberMetaData mmd, Object value, FieldRole fieldRole) {
        if (value == null) {
            return null;
        }
    
        boolean optional = (mmd != null && Optional.class.isAssignableFrom(mmd.getType()));
        Class<?> memberType = getMemberTypeForFieldRole(ec, mmd, fieldRole, optional);
    
        if (mmd != null && mmd.hasCollection() && !optional && fieldRole == FieldRole.ROLE_FIELD) {
            Collection<Object> coll;
            try {
                Class<?> instanceType = SCOUtils.getContainerInstanceType(mmd.getType(), mmd.getOrderMetaData() != null);
                coll = (Collection<Object>) instanceType.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new NucleusDataStoreException(e.getMessage(), e);
            }
            for (int i = 0; i < Array.getLength(value); i++) {
                coll.add(getFieldValueFromStored(ec, mmd, Array.get(value, i), FieldRole.ROLE_COLLECTION_ELEMENT));
            }
            return coll;
        } else if (mmd != null && mmd.hasArray() && fieldRole == FieldRole.ROLE_FIELD) {
            Object array = Array.newInstance(mmd.getType().getComponentType(), Array.getLength(value));
            for (int i = 0; i < Array.getLength(value); i++) {
                Array.set(array, i, getFieldValueFromStored(ec, mmd, Array.get(value, i), FieldRole.ROLE_ARRAY_ELEMENT));
            }
            return array;
        }
    
        if (isNativelySupported(memberType) && memberType.isInstance(value)) {
            return value;
        }
    
        if (memberType.isEnum()) {
            return EnumConversionHelper.getEnumForStoredValue(mmd, fieldRole, value, ec.getClassLoaderResolver());
        }
    
        // === FIX START ===
        // Use raw TypeConverter and rely on instanceof checks for type safety.
        TypeConverter strConv = ec.getTypeManager().getTypeConverterForType(memberType, String.class);
        if (strConv != null && value instanceof String) {
            return strConv.toMemberType(value);
        }
    
        TypeConverter longConv = ec.getTypeManager().getTypeConverterForType(memberType, Long.class);
        if (longConv != null && value instanceof Long) {
            return longConv.toMemberType(value);
        }
        // === FIX END ===

        // Fallback for types that might have been stored via .toString()
        if (value instanceof String && !memberType.equals(String.class)) {
            try {
                return ec.getClassLoaderResolver().classForName(memberType.getName()).getConstructor(String.class).newInstance(value);
            } catch (Exception e) {
                // Ignore if no String constructor is available; the value will be returned as-is.
            }
        }
    
        return value;
    }

    private static Class<?> getMemberTypeForFieldRole(ExecutionContext ec, AbstractMemberMetaData mmd, FieldRole fieldRole, boolean optional) {
        if (mmd == null) return Object.class;
    
        if (optional) {
            return ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
        }
    
        switch (fieldRole) {
            case ROLE_COLLECTION_ELEMENT:
                return ec.getClassLoaderResolver().classForName(mmd.getCollection().getElementType());
            case ROLE_ARRAY_ELEMENT:
                return ec.getClassLoaderResolver().classForName(mmd.getArray().getElementType());
            case ROLE_MAP_KEY:
                return ec.getClassLoaderResolver().classForName(mmd.getMap().getKeyType());
            case ROLE_MAP_VALUE:
                return ec.getClassLoaderResolver().classForName(mmd.getMap().getValueType());
            default:
                return mmd.getType();
        }
    }

    private static boolean isNativelySupported(Class<?> type) {
        return type.isPrimitive() ||
               Byte.class.isAssignableFrom(type) ||
               Boolean.class.isAssignableFrom(type) ||
               Character.class.isAssignableFrom(type) ||
               Double.class.isAssignableFrom(type) ||
               Float.class.isAssignableFrom(type) ||
               Integer.class.isAssignableFrom(type) ||
               Long.class.isAssignableFrom(type) ||
               Short.class.isAssignableFrom(type) ||
               String.class.isAssignableFrom(type);
    }

    private static Object convertArrayToStorableArray(Object[] objArray, AbstractMemberMetaData mmd) {
        if (objArray == null || objArray.length == 0) {
            return null;
        }

        Object firstElem = null;
        for(Object obj : objArray){
            if(obj != null){
                firstElem = obj;
                break;
            }
        }

        if (firstElem == null) {
            return objArray; // Cannot determine type if all elements are null
        }

        Class<?> componentType = firstElem.getClass();
        Object array;

        if (ClassUtils.isPrimitiveWrapperType(componentType.getName())) {
            Class<?> primType = ClassUtils.getPrimitiveTypeForType(componentType);
            array = Array.newInstance(primType, objArray.length);
            for (int i = 0; i < objArray.length; i++) {
                if(objArray[i] != null) Array.set(array, i, objArray[i]);
            }
        } else if (String.class.isAssignableFrom(componentType)) {
             array = Array.newInstance(componentType, objArray.length);
             System.arraycopy(objArray, 0, array, 0, objArray.length);
        } else {
            throw new NucleusException("Field " + mmd.getFullFieldName() +
                " has an array/collection of " + componentType.getName() + 
                " which is not a supported type for persistence in Neo4j properties.");
        }
        return array;
    }
}

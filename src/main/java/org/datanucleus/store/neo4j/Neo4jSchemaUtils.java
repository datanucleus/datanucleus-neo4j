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
package org.datanucleus.store.neo4j;

import org.datanucleus.ExecutionContext;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.ColumnMetaData;
import org.datanucleus.metadata.EmbeddedMetaData;
import org.datanucleus.store.StoreData;
import org.datanucleus.store.StoreManager;
import org.datanucleus.store.schema.table.Column;
import org.datanucleus.store.schema.table.SurrogateColumnType;
import org.datanucleus.store.schema.table.Table;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for handling DataNucleus schema and metadata constructs in Neo4j.
 */
public final class Neo4jSchemaUtils {

    private Neo4jSchemaUtils() {
        // Private constructor
    }

    /**
     * Returns the name for the surrogate version column, if the class is versioned.
     * @param cmd Metadata for the class.
     * @param storeMgr The Store Manager
     * @return The surrogate version column name.
     */
    public static String getSurrogateVersionName(AbstractClassMetaData cmd, StoreManager storeMgr) {
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null || sd.getTable() == null) {
            return "version"; // Fallback
        }
        return sd.getTable().getSurrogateColumn(SurrogateColumnType.VERSION).getName();
    }

    /**
     * Returns the name for the surrogate discriminator column.
     * @param cmd Metadata for the class.
     * @param storeMgr The Store Manager
     * @return The surrogate discriminator column name.
     */
    public static String getSurrogateDiscriminatorName(AbstractClassMetaData cmd, StoreManager storeMgr) {
        StoreData sd = storeMgr.getStoreDataForClass(cmd.getFullClassName());
        if (sd == null || sd.getTable() == null) {
            return "DN_DISCRIM"; // Fallback
        }
        return sd.getTable().getSurrogateColumn(SurrogateColumnType.DISCRIMINATOR).getName();
    }

    /**
     * Retrieves all labels for a given class, including labels from all superclasses.
     * @param cmd Metadata for the class.
     * @param storeMgr The StoreManager.
     * @return A list of all applicable labels.
     */
    public static List<String> getLabelsForClass(AbstractClassMetaData cmd, StoreManager storeMgr) {
        List<String> labels = new ArrayList<>();
        labels.add(cmd.getName());
        AbstractClassMetaData superCmd = cmd;
        while ((superCmd = superCmd.getSuperAbstractClassMetaData()) != null) {
            labels.add(superCmd.getName());
        }
        return labels;
    }

    /**
     * Formats a label name to be safe for use in Cypher.
     * @param label The raw label name.
     * @return A Cypher-safe label name.
     */
    public static String getLabelName(String label) {
        return "`" + label + "`";
    }

    /**
     * Checks if a class is marked to be persisted as an attributed relationship.
     */
    public static boolean classIsAttributedRelation(AbstractClassMetaData cmd) {
        if (cmd.hasExtension(Neo4jStoreManager.METADATA_ATTRIBUTED_RELATION)) {
            return "true".equalsIgnoreCase(cmd.getValueForExtension(Neo4jStoreManager.METADATA_ATTRIBUTED_RELATION));
        }
        return false;
    }

    /**
     * Gets the property name for a field within an embedded object.
     */
    public static String getPropertyNameForEmbeddedField(AbstractMemberMetaData ownerMmd, int fieldNumber) {
        EmbeddedMetaData embmd = ownerMmd.getEmbeddedMetaData();
        if (embmd == null) return null;
        AbstractMemberMetaData embMmd = embmd.getMemberMetaData().get(fieldNumber);
        if (embMmd == null) return null;
        ColumnMetaData[] colmds = embMmd.getColumnMetaData();
        if (colmds != null && colmds.length > 0 && colmds[0].getName() != null) {
            return colmds[0].getName();
        }
        return embMmd.getName();
    }
    
    /**
     * Constructs a Cypher query string for JDOQL/JPQL queries.
     */
    public static String getCypherTextForQuery(ExecutionContext ec, AbstractClassMetaData cmd, String candidateAlias,
            boolean subclasses, String filterText, String resultText, String orderText, Long rangeFromIncl, Long rangeToExcl) {
        StoreData sd = ec.getStoreManager().getStoreDataForClass(cmd.getFullClassName());
        Table table = (sd != null) ? sd.getTable() : null;
        boolean attributedRelation = classIsAttributedRelation(cmd);
        String alias = (candidateAlias != null) ? candidateAlias : (attributedRelation ? "r" : "n");
        String indexClassName = cmd.getFullClassName() + (subclasses ? "" : "-EXCLUSIVE");
        String startClause = "START " + alias + (attributedRelation ? "=relationship:" : "=node:") + 
                             Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX + 
                             "(`" + Neo4jStoreManager.PROPCONTAINER_TYPE_INDEX_KEY + "`=\"" + indexClassName + "\")";
        StringBuilder whereClause = new StringBuilder();
        addWhereCondition(whereClause, filterText, false);
        if (table != null) {
            Column multitenancyCol = table.getSurrogateColumn(SurrogateColumnType.MULTITENANCY);
            if (multitenancyCol != null && ec.getTenantId() != null) {
                addWhereCondition(whereClause, alias + ".`" + multitenancyCol.getName() + "` = \"" + ec.getTenantId() + "\"", true);
            }
            Column softDeleteCol = table.getSurrogateColumn(SurrogateColumnType.SOFTDELETE);
            if (softDeleteCol != null) {
                addWhereCondition(whereClause, alias + ".`" + softDeleteCol.getName() + "` = false", true);
            }
        }
        String returnClause = "RETURN " + ((resultText != null) ? resultText : alias);
        String orderClause = (orderText != null) ? "ORDER BY " + orderText : "";
        StringBuilder rangeClause = new StringBuilder();
        if (rangeFromIncl != null) {
            rangeClause.append(" SKIP ").append(rangeFromIncl);
        }
        if (rangeToExcl != null) {
            long limit = (rangeFromIncl != null) ? rangeToExcl - rangeFromIncl : rangeToExcl;
            rangeClause.append(" LIMIT ").append(limit);
        }
        return String.format("%s %s %s %s %s", startClause, whereClause, returnClause, orderClause, rangeClause.toString()).trim();
    }

    private static void addWhereCondition(StringBuilder whereClause, String condition, boolean addParentheses) {
        if (condition == null || condition.trim().isEmpty()) return;
        if (whereClause.length() == 0) {
            whereClause.append("WHERE ");
        } else {
            whereClause.append(" AND ");
        }
        if (addParentheses) {
            whereClause.append("(").append(condition).append(")");
        } else {
            whereClause.append(condition);
        }
    }
}
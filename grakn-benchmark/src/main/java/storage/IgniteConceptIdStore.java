/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package storage;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.SchemaConcept;
import com.google.common.collect.ImmutableMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static ai.grakn.concept.AttributeType.DataType.LONG;
import static ai.grakn.concept.AttributeType.DataType.STRING;
import static ai.grakn.concept.AttributeType.DataType.BOOLEAN;
import static ai.grakn.concept.AttributeType.DataType.DOUBLE;
import static ai.grakn.concept.AttributeType.DataType.INTEGER;
import static ai.grakn.concept.AttributeType.DataType.FLOAT;
import static ai.grakn.concept.AttributeType.DataType.DATE;

/**
 * Stores identifiers for all concepts in a Grakn
 */
public class IgniteConceptIdStore implements IdStoreInterface {

    private final HashSet<String> entityTypeLabels;
    private final HashSet<String> relationshipTypeLabels;
//    private final HashMap<String, AttributeType.DataType<?>> attributeTypeLabels; // typeLabel, datatype
    private final HashMap<java.lang.String, AttributeType.DataType<?>> attributeTypeLabels; // typeLabel, datatype

    private Connection conn;
    private HashSet<String> allTypeLabels;
    private final String cachingMethod = "REPLICATED";

    public static final ImmutableMap<AttributeType.DataType<?>, String> DATATYPE_MAPPING = ImmutableMap.<AttributeType.DataType<?>, String>builder()
            .put(STRING, "VARCHAR")
            .put(BOOLEAN, "BOOLEAN")
            .put(LONG, "LONG")
            .put(DOUBLE, "DOUBLE")
            .put(INTEGER, "INTEGER")
            .put(FLOAT, "FLOAT")
            .put(DATE, "DATE")
            .build();

    public IgniteConceptIdStore(HashSet<EntityType> entityTypes,
                                HashSet<RelationshipType> relationshipTypes,
                                HashSet<AttributeType> attributeTypes) {

        // TODO Need to do it this way since we need the datatypes of the attributes

        this.entityTypeLabels = this.getTypeLabels(entityTypes);
        this.relationshipTypeLabels = this.getTypeLabels(relationshipTypes);
        this.attributeTypeLabels = this.getAttributeTypeLabels(attributeTypes);

        this.allTypeLabels = this.getAllTypeLabels();

        try {
            clean(this.allTypeLabels);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Register JDBC driver.
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        // Open JDBC connection.
        try {
            this.conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // Create database tables.
        // TODO different table datatypes depending on attribute datatype
        for (String typeLabel : this.entityTypeLabels) {
            this.createConceptIdTable(typeLabel);
        }

        for (String typeLabel : this.relationshipTypeLabels) {
            this.createConceptIdTable(typeLabel);
        }

//        for (String typeLabel : this.relationshipTypeLabels) {
        for (Map.Entry<String, AttributeType.DataType<?>> entry : this.attributeTypeLabels.entrySet()) {
            String typeLabel = entry.getKey();
            AttributeType.DataType<?> datatype = entry.getValue();

            String dbDatatype = DATATYPE_MAPPING.get(datatype);
            this.createTable(typeLabel, dbDatatype);
        }
    }

    private <T extends SchemaConcept> HashSet<String> getTypeLabels(Set<T> conceptTypes) {
        HashSet<String> typeLabels = new HashSet<>();
        for (T conceptType : conceptTypes) {
            typeLabels.add(conceptType.getLabel().toString());
        }
        return typeLabels;
    }

    private HashMap<String, AttributeType.DataType<?>> getAttributeTypeLabels(Set<AttributeType> conceptTypes) {
        HashMap<String, AttributeType.DataType<?>> typeLabels = new HashMap<>();
        for (AttributeType conceptType : conceptTypes) {
            String label = conceptType.getLabel().toString();

            AttributeType.DataType<?> datatype = conceptType.getDataType();
            typeLabels.put(label, datatype);
        }
        return typeLabels;
    }

    private HashSet<String> getAllTypeLabels() {
        HashSet<String> allLabels = new HashSet<>();
        allLabels.addAll(this.entityTypeLabels);
        allLabels.addAll(this.relationshipTypeLabels);
        allLabels.addAll(this.attributeTypeLabels.keySet());
        return allLabels;
    }

    private void createConceptIdTable(String typeLabel) {
        createTable(typeLabel, "VARCHAR");
    }

    private void createTable(String typeLabel, String sqlDatatypeName) {
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + typeLabel + " (" +
                    " id " + sqlDatatypeName + " PRIMARY KEY, " +
                    "nothing LONG) " +
                    " WITH \"template=" + cachingMethod + "\"");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void add(Concept concept) {

        Label conceptTypeLabel = concept.asThing().type().getLabel();
        String conceptId = concept.asThing().getId().toString(); // TODO use the value instead for attributes

        try (PreparedStatement stmt = this.conn.prepareStatement(
                "INSERT INTO " + conceptTypeLabel + " (id, ) VALUES (?, )")) {

            stmt.setString(1, conceptId);
            stmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
    [{ LIMIT expression [OFFSET expression]
    [SAMPLE_SIZE rowCountInt]} | {[OFFSET expression {ROW | ROWS}]
    [{FETCH {FIRST | NEXT} expression {ROW | ROWS} ONLY}]}]
     */

    public <T> T get(String typeLabel, Class<T> datatype, int offset) {
        //TODO the datatype being used here is important when accessing attributes

        String sql = "SELECT id FROM " + typeLabel +
                " OFFSET " + offset +
                " FETCH FIRST ROW ONLY";
//        ResultSet rs = this.runQuery(sql);
        int columnIndex = 1;
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {

                if (rs != null && rs.next()) { // Need to do this to increment one line in the ResultSet
                    if (datatype == ConceptId.class) {
                        return datatype.cast(ConceptId.of(rs.getString(columnIndex)));

                    } else if (datatype == String.class) {
                        return datatype.cast(rs.getString(columnIndex));

                    } else if (datatype == Double.class) {
                        return datatype.cast(rs.getDouble(columnIndex));

                    } else if (datatype == Integer.class) {
                        return datatype.cast(rs.getInt(columnIndex));

                    } else if (datatype == Long.class) {
                        return datatype.cast(rs.getLong(columnIndex));

                    } else if (datatype == Boolean.class) {
                        return datatype.cast(rs.getBoolean(columnIndex));

                    } else if (datatype == Date.class) {
                        return datatype.cast(rs.getDate(columnIndex));
                    } else {
                        throw new UnsupportedOperationException(String.format("Datatype %s isn't supported by Grakn", datatype));
                    }
                } else {
                    return null;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public int getConceptCount(String typeLabel) {

        String sql = "SELECT COUNT(1) FROM " + typeLabel;
//        ResultSet rs = this.runQuery(sql);

        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {

                if (rs != null && rs.next()) { // Need to do this to increment one line in the ResultSet
                    return rs.getInt(1);
                } else {
                    return 0;
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public int total() {
        int total = 0;
        for (String typeLabel : this.allTypeLabels) {
            total += this.getConceptCount(typeLabel);
        }
        return total;
    }

    private ResultSet runQuery(String sql) {
        try (Statement stmt = conn.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(sql)) {
                return rs;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Clean all elements in the storage
     */
    public static void clean(Set<String> typeLabels) throws SQLException {
        // TODO figure out how to drop all tables
        for (String typeLabel : typeLabels) {
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
            try (PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS " + typeLabel)) {
                stmt.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}

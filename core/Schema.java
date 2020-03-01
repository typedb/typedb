/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package grakn.core.core;

import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.Entity;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.GraknConceptException;
import grakn.core.kb.concept.api.Label;
import grakn.core.kb.concept.api.LabelId;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Rule;
import grakn.core.kb.concept.api.SchemaConcept;
import grakn.core.kb.concept.api.Type;
import graql.lang.Graql;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import java.util.Map;

import static grakn.common.util.Collections.map;
import static grakn.common.util.Collections.pair;
import static grakn.core.common.exception.ErrorMessage.INVALID_IMPLICIT_TYPE;

/**
 * A type enum which restricts the types of links/concepts which can be created
 */
public final class Schema {
    private final static String PREFIX_VERTEX = "V";
    private final static String PREFIX_EDGE = "E";

    private Schema() {
        throw new UnsupportedOperationException();
    }

    public static ConceptId conceptIdFromVertexId(Object vertexId) {
        return ConceptId.of(PREFIX_VERTEX + vertexId);
    }

    public static ConceptId conceptId(Element element) {
        String prefix = element instanceof Edge ? PREFIX_EDGE : PREFIX_VERTEX;
        return ConceptId.of(prefix + element.id().toString());
    }

    public static String elementId(ConceptId conceptId) {
        return conceptId.getValue().substring(1);
    }

    public static boolean validateConceptId(ConceptId conceptId) throws GraknConceptException {
        try {
            if (!isEdgeId(conceptId)) {
                Long.parseLong(elementId(conceptId));
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isEdgeId(ConceptId conceptId) {
        return conceptId.getValue().startsWith(Schema.PREFIX_EDGE);
    }

    /**
     * The different types of edges between vertices
     */
    public enum EdgeLabel {
        ISA("isa"),
        SUB("sub"),
        RELATES("relates"),
        PLAYS("plays"),
        POSITIVE_HYPOTHESIS("positive-hypothesis"),
        NEGATIVE_HYPOTHESIS("negative-hypothesis"),
        CONCLUSION("conclusion"),
        ROLE_PLAYER("role-player"),
        ATTRIBUTE("attribute"),
        SHARD("shard");

        private final String label;

        EdgeLabel(String l) {
            label = l;
        }

        @CheckReturnValue
        public String getLabel() {
            return label;
        }
    }

    /**
     * The concepts which represent our internal schema
     */
    public enum MetaSchema {
        THING(Graql.Token.Type.THING.toString(), 1),
        ENTITY(Graql.Token.Type.ENTITY.toString(), 2),
        ROLE(Graql.Token.Type.ROLE.toString(), 3),
        ATTRIBUTE(Graql.Token.Type.ATTRIBUTE.toString(), 4),
        RELATION(Graql.Token.Type.RELATION.toString(), 5),
        RULE(Graql.Token.Type.RULE.toString(), 6);

        private final Label label;
        private final LabelId id;

        MetaSchema(String s, int i) {
            label = Label.of(s);
            id = LabelId.of(i);
        }

        @CheckReturnValue
        public Label getLabel() {
            return label;
        }

        @CheckReturnValue
        public LabelId getId() {
            return id;
        }

        @CheckReturnValue
        public static boolean isMetaLabel(Label label) {
            return valueOf(label) != null;
        }

        @Nullable
        @CheckReturnValue
        public static MetaSchema valueOf(Label label) {
            for (MetaSchema metaSchema : MetaSchema.values()) {
                if (metaSchema.getLabel().equals(label)) return metaSchema;
            }
            return null;
        }
    }

    /**
     * Base Types reflecting the possible objects in the concept
     */
    public enum BaseType {
        //Schema Concepts
        SCHEMA_CONCEPT(SchemaConcept.class),
        TYPE(Type.class),
        ENTITY_TYPE(EntityType.class),
        RELATION_TYPE(RelationType.class),
        ATTRIBUTE_TYPE(AttributeType.class),
        ROLE(Role.class),
        RULE(Rule.class),

        //Instances
        RELATION(Relation.class),
        ENTITY(Entity.class),
        ATTRIBUTE(Attribute.class),

        //Internal
        SHARD(Vertex.class),
        CONCEPT(Concept.class);//No concept actually has this base type. This is used to prevent string hardcoding

        private final Class classType;

        BaseType(Class classType) {
            this.classType = classType;
        }

        @CheckReturnValue
        public Class getClassType() {
            return classType;
        }
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum VertexProperty {
        // Schema concept properties
        SCHEMA_LABEL(String.class), LABEL_ID(Integer.class), INSTANCE_COUNT(Long.class), TYPE_SHARD_CHECKPOINT(Long.class), IS_ABSTRACT(Boolean.class),

        // Attribute schema concept properties
        REGEX(String.class), DATA_TYPE(String.class),

        // Attribute concept properties
        INDEX(String.class),

        // Reified relations' ID (exported from a previously non-reified edge ID)
        EDGE_RELATION_ID(String.class),

        // Properties on all Concept vertices
        THING_TYPE_LABEL_ID(Integer.class), IS_INFERRED(Boolean.class),

        // Misc. properties
        CURRENT_LABEL_ID(Integer.class), RULE_WHEN(String.class), RULE_THEN(String.class), CURRENT_SHARD(String.class),

        // Relation properties
        IS_IMPLICIT(Boolean.class),

        //Supported Data Types
        VALUE_STRING(String.class), VALUE_LONG(Long.class),
        VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class),
        VALUE_INTEGER(Integer.class), VALUE_FLOAT(Float.class),
        VALUE_DATE(Long.class);

        private final Class dataType;

        private static Map<AttributeType.DataType, VertexProperty> dataTypeVertexProperty = map(
                pair(AttributeType.DataType.BOOLEAN, VertexProperty.VALUE_BOOLEAN),
                pair(AttributeType.DataType.DATE, VertexProperty.VALUE_DATE),
                pair(AttributeType.DataType.DOUBLE, VertexProperty.VALUE_DOUBLE),
                pair(AttributeType.DataType.FLOAT, VertexProperty.VALUE_FLOAT),
                pair(AttributeType.DataType.INTEGER, VertexProperty.VALUE_INTEGER),
                pair(AttributeType.DataType.LONG, VertexProperty.VALUE_LONG),
                pair(AttributeType.DataType.STRING, VertexProperty.VALUE_STRING)
        );

        VertexProperty(Class dataType) {
            this.dataType = dataType;
        }

        @CheckReturnValue
        public Class getPropertyClass() {
            return dataType;
        }

        // TODO: This method feels out of place
        public static VertexProperty ofDataType(AttributeType.DataType dataType) {
            return dataTypeVertexProperty.get(dataType);
        }
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        RELATION_ROLE_OWNER_LABEL_ID(Integer.class),
        RELATION_ROLE_VALUE_LABEL_ID(Integer.class),
        ROLE_LABEL_ID(Integer.class),
        RELATION_TYPE_LABEL_ID(Integer.class),
        REQUIRED(Boolean.class),
        IS_INFERRED(Boolean.class);

        private final Class dataType;

        EdgeProperty(Class dataType) {
            this.dataType = dataType;
        }

        @CheckReturnValue
        public Class getPropertyClass() {
            return dataType;
        }
    }

    /**
     * This stores the schema which is required when implicitly creating roles for the has-Attribute methods
     */
    public enum ImplicitType {
        /**
         * Reserved character used by all implicit Types
         */
        RESERVED("@"),

        /**
         * The label of the generic has-Attribute relation, used for attaching Attributes to instances with the 'has' syntax
         */
        HAS("@has-%s"),

        /**
         * The label of a role in has-Attribute, played by the owner of the Attribute
         */
        HAS_OWNER("@has-%s-owner"),

        /**
         * The label of a role in has-Attribute, played by the Attribute
         */
        HAS_VALUE("@has-%s-value"),

        /**
         * The label of the generic key relation, used for attaching Attributes to instances with the 'has' syntax and additionally constraining them to be unique
         */
        KEY("@key-%s"),

        /**
         * The label of a role in key, played by the owner of the key
         */
        KEY_OWNER("@key-%s-owner"),

        /**
         * The label of a role in key, played by the Attribute
         */
        KEY_VALUE("@key-%s-value");

        private final String label;

        ImplicitType(String label) {
            this.label = label;
        }

        @CheckReturnValue
        public Label getLabel(Label attributeType) {
            return attributeType.map(attribute -> String.format(label, attribute));
        }

        @CheckReturnValue
        public Label getLabel(String attributeType) {
            return Label.of(String.format(label, attributeType));
        }

        @CheckReturnValue
        public String getValue() {
            return label;
        }

        /**
         * Helper method which converts the implicit type label back into the original label from which is was built.
         *
         * @param implicitType the implicit type label
         * @return The original label which was used to build this type
         */
        @CheckReturnValue
        public static Label explicitLabel(Label implicitType) {
            if (!(implicitType.getValue().startsWith("@key") || implicitType.getValue().startsWith("@has"))) {
                throw new IllegalArgumentException(INVALID_IMPLICIT_TYPE.getMessage(implicitType));
            }

            int endIndex = implicitType.getValue().length();
            // We need to exclude the scenario where the user literally names their attributes 'value' or 'owner'
            // which will result in @has-value, @has-owner, @key-value, @key-owner as legitimate implicit-relation names
            if (implicitType.getValue().length() > 10 && (implicitType.getValue().endsWith("-value") || implicitType.getValue().endsWith("-owner"))) {
                endIndex = implicitType.getValue().lastIndexOf("-");
            }

            //return the Label without the `@has-`or '@key-' prefix
            return Label.of(implicitType.getValue().substring(5, endIndex));
        }

        @CheckReturnValue
        public static boolean isKey(Label implicitType) {
            return implicitType.getValue().startsWith("@key");
        }
    }

    /**
     * @param label The AttributeType label
     * @param value The value of the Attribute
     * @return A unique id for the Attribute
     */
    @CheckReturnValue
    public static String generateAttributeIndex(Label label, String value) {
        //TODO trim it down in the future
        return Schema.BaseType.ATTRIBUTE.name() + "-" + label + "-" + value;
    }
}

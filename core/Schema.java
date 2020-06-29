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

/**
 * A type enum which restricts the types of links/concepts which can be created
 */
public final class Schema {
    private final static String PREFIX_VERTEX = "V";

    private Schema() {
        throw new UnsupportedOperationException();
    }

    public static ConceptId conceptIdFromVertexId(Object vertexId) {
        return ConceptId.of(PREFIX_VERTEX + vertexId);
    }

    public static ConceptId conceptId(Vertex element) {
        return ConceptId.of(PREFIX_VERTEX + element.id().toString());
    }

    public static String elementId(ConceptId conceptId) {
        return conceptId.getValue().substring(1);
    }

    public static boolean validateConceptId(ConceptId conceptId) throws GraknConceptException {
        try {
            Long.parseLong(elementId(conceptId));
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    /**
     * The different types of edges between vertices
     */
    public enum EdgeLabel {
        ISA("isa"),
        SUB("sub"),
        RELATES("relates"),
        PLAYS("plays"),
        HAS("schema-has"), // has schema edge
        KEY("schema-key"), // key schema edge -- "key" is a reserved keyword!
        POSITIVE_HYPOTHESIS("positive-hypothesis"),
        NEGATIVE_HYPOTHESIS("negative-hypothesis"),
        CONCLUSION("conclusion"),
        ROLE_PLAYER("role-player"),
        ATTRIBUTE("attribute"), // attribute ownership edge
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
        SCHEMA_LABEL(String.class), LABEL_ID(Integer.class), INSTANCE_COUNT(Long.class), OWNERSHIP_COUNT(Long.class), TYPE_SHARD_CHECKPOINT(Long.class), IS_ABSTRACT(Boolean.class),

        // Attribute schema concept properties
        REGEX(String.class), VALUE_TYPE(String.class),

        // Attribute concept properties
        INDEX(String.class),

        // Properties on all Concept vertices
        THING_TYPE_LABEL_ID(Integer.class), IS_INFERRED(Boolean.class),

        // Misc. properties
        CURRENT_LABEL_ID(Integer.class), RULE_WHEN(String.class), RULE_THEN(String.class), CURRENT_SHARD(String.class),

        //Supported Value Types
        VALUE_STRING(String.class), VALUE_LONG(Long.class),
        VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class),
        VALUE_INTEGER(Integer.class), VALUE_FLOAT(Float.class),
        VALUE_DATE(Long.class);

        private final Class valueType;

        private static Map<AttributeType.ValueType, VertexProperty> valueTypeVertexProperty = map(
                pair(AttributeType.ValueType.BOOLEAN, VertexProperty.VALUE_BOOLEAN),
                pair(AttributeType.ValueType.DATETIME, VertexProperty.VALUE_DATE),
                pair(AttributeType.ValueType.DOUBLE, VertexProperty.VALUE_DOUBLE),
                pair(AttributeType.ValueType.FLOAT, VertexProperty.VALUE_FLOAT),
                pair(AttributeType.ValueType.INTEGER, VertexProperty.VALUE_INTEGER),
                pair(AttributeType.ValueType.LONG, VertexProperty.VALUE_LONG),
                pair(AttributeType.ValueType.STRING, VertexProperty.VALUE_STRING)
        );

        VertexProperty(Class valueType) {
            this.valueType = valueType;
        }

        @CheckReturnValue
        public Class getPropertyClass() {
            return valueType;
        }

        // TODO: This method feels out of place
        public static VertexProperty ofValueType(AttributeType.ValueType valueType) {
            return valueTypeVertexProperty.get(valueType);
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
        IS_INFERRED(Boolean.class),
        // forwards compatibility
        ATTRIBUTE_OWNED_LABEL_ID(Integer.class),
        ATTRIBUTE_OWNER_LABEL_ID(Integer.class);

        private final Class valueType;

        EdgeProperty(Class valueType) {
            this.valueType = valueType;
        }

        @CheckReturnValue
        public Class getPropertyClass() {
            return valueType;
        }
    }

    /**
     * @param label The AttributeType label
     * @param value The computed value of the Attribute
     * @return A unique id for the Attribute
     */
    @CheckReturnValue
    public static String generateAttributeIndex(Label label, String value) {
        //TODO trim it down in the future
        return Schema.BaseType.ATTRIBUTE.name() + "-" + label + "-" + value;
    }
}

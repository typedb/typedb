/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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
 */

package grakn.core.graql.internal;

import grakn.core.graql.concept.Attribute;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Entity;
import grakn.core.graql.concept.EntityType;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.LabelId;
import grakn.core.graql.concept.Relation;
import grakn.core.graql.concept.RelationType;
import grakn.core.graql.concept.Role;
import grakn.core.graql.concept.Rule;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.concept.Type;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;

import static grakn.core.common.exception.ErrorMessage.INVALID_IMPLICIT_TYPE;

/**
 * A type enum which restricts the types of links/concepts which can be created
 *
 */
public final class Schema {
    public final static String PREFIX_VERTEX = "V";
    public final static String PREFIX_EDGE = "E";

    private Schema() {
        throw new UnsupportedOperationException();
    }

    /**
     * The different types of edges between vertices
     */
    public enum EdgeLabel {
        ISA("isa"),
        SUB("sub"),
        RELATES("relates"),
        PLAYS("plays"),
        HYPOTHESIS("hypothesis"),
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
        THING("thing", 1),
        ENTITY("entity", 2),
        ROLE("role", 3),
        ATTRIBUTE("attribute", 4),
        RELATIONSHIP("relationship", 5),
        RULE("rule", 6);

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
        public LabelId getId(){
            return id;
        }

        @CheckReturnValue
        public static boolean isMetaLabel(Label label) {
            return valueOf(label) != null;
        }

        @Nullable
        @CheckReturnValue
        public static MetaSchema valueOf(Label label){
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
        RELATIONSHIP_TYPE(RelationType.class),
        ATTRIBUTE_TYPE(AttributeType.class),
        ROLE(Role.class),
        RULE(Rule.class),

        //Instances
        RELATIONSHIP(Relation.class),
        ENTITY(Entity.class),
        ATTRIBUTE(Attribute.class),

        //Internal
        SHARD(Vertex.class),
        CONCEPT(Concept.class);//No concept actually has this base type. This is used to prevent string hardcoding

        private final Class classType;

        BaseType(Class classType){
            this.classType = classType;
        }

        @CheckReturnValue
        public Class getClassType(){
            return classType;
        }
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum VertexProperty {
        //Unique Properties
        SCHEMA_LABEL(String.class), INDEX(String.class), ID(String.class), LABEL_ID(Integer.class),

        //Other Properties
        THING_TYPE_LABEL_ID(Integer.class),
        IS_ABSTRACT(Boolean.class), IS_IMPLICIT(Boolean.class), IS_INFERRED(Boolean.class),
        REGEX(String.class), DATA_TYPE(String.class), CURRENT_LABEL_ID(Integer.class),
        RULE_WHEN(String.class), RULE_THEN(String.class), CURRENT_SHARD(String.class),

        //Supported Data Types
        VALUE_STRING(String.class), VALUE_LONG(Long.class),
        VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class),
        VALUE_INTEGER(Integer.class), VALUE_FLOAT(Float.class),
        VALUE_DATE(Long.class);

        private final Class dataType;

        VertexProperty(Class dataType) {
            this.dataType = dataType;
        }

        @CheckReturnValue
        public Class getDataType() {
            return dataType;
        }
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        RELATIONSHIP_ROLE_OWNER_LABEL_ID(Integer.class),
        RELATIONSHIP_ROLE_VALUE_LABEL_ID(Integer.class),
        ROLE_LABEL_ID(Integer.class),
        RELATIONSHIP_TYPE_LABEL_ID(Integer.class),
        REQUIRED(Boolean.class),
        IS_INFERRED(Boolean.class);

        private final Class dataType;

        EdgeProperty(Class dataType) {
            this.dataType = dataType;
        }

        @CheckReturnValue
        public Class getDataType() {
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
         * The label of the generic has-Attribute relationship, used for attaching Attributes to instances with the 'has' syntax
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
         * The label of the generic key relationship, used for attaching Attributes to instances with the 'has' syntax and additionally constraining them to be unique
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
        public String getValue(){
            return label;
        }

        /**
         * Helper method which converts the implicit type label back into the original label from which is was built.
         *
         * @param implicitType the implicit type label
         * @return The original label which was used to build this type
         */
        @CheckReturnValue
        public static Label explicitLabel(Label implicitType){
            if(!implicitType.getValue().startsWith("key") && implicitType.getValue().startsWith("has")){
                throw new IllegalArgumentException(INVALID_IMPLICIT_TYPE.getMessage(implicitType));
            }

            int endIndex = implicitType.getValue().length();
            if(implicitType.getValue().endsWith("-value") || implicitType.getValue().endsWith("-owner")) {
                endIndex = implicitType.getValue().lastIndexOf("-");
            }

            //return the Label without the `@has-`or '@key-' prefix
            return Label.of(implicitType.getValue().substring(5, endIndex));
        }
    }

    /**
     *
     * @param label The AttributeType label
     * @param value The value of the Attribute
     * @return A unique id for the Attribute
     */
    @CheckReturnValue
    public static String generateAttributeIndex(Label label, String value){
        return Schema.BaseType.ATTRIBUTE.name() + "-" + label + "-" + value;
    }
}

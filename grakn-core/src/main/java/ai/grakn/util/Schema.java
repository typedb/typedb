/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.util;

import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeLabel;

import java.util.Date;

/**
 * A type enum which restricts the types of links/concepts which can be created
 *
 * @author Filipe Teixeira
 */
public final class Schema {
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
        HAS_SCOPE("has-scope"),
        CASTING("casting"),
        ROLE_PLAYER("role-player"),
        HYPOTHESIS("hypothesis"),
        CONCLUSION("conclusion"),
        SHORTCUT("shortcut");

        private final String label;

        EdgeLabel(String l) {
            label = l;
        }

        public String getLabel() {
            return label;
        }

        public static EdgeLabel getEdgeLabel(String label) {
            for (EdgeLabel edgeLabel : EdgeLabel.values()) {
                if (edgeLabel.getLabel().equals(label)) {
                    return edgeLabel;
                }
            }
            return null;
        }
    }

    /**
     * The concepts which represent our internal schema
     */
    public enum MetaSchema {
        CONCEPT("concept"),
        ENTITY("entity"),
        ROLE("role"),
        RESOURCE("resource"),
        RELATION("relation"),
        RULE("rule"),
        INFERENCE_RULE("inference-rule"),
        CONSTRAINT_RULE("constraint-rule");


        private final TypeLabel label;

        MetaSchema(String i) {
            label = TypeLabel.of(i);
        }

        public TypeLabel getLabel() {
            return label;
        }

        public static boolean isMetaLabel(TypeLabel label) {
            for (MetaSchema metaSchema : MetaSchema.values()) {
                if (metaSchema.getLabel().equals(label)) return true;
            }
            return false;
        }
    }

    /**
     * Base Types reflecting the possible objects in the concept
     */
    public enum BaseType {
        //Types
        TYPE(Type.class),
        ROLE_TYPE(RoleType.class),
        RELATION_TYPE(RelationType.class),
        RESOURCE_TYPE(ResourceType.class),
        ENTITY_TYPE(EntityType.class),
        RULE_TYPE(RuleType.class),

        //Instances
        RELATION(Relation.class),
        CASTING(Instance.class),
        ENTITY(Entity.class),
        RESOURCE(Resource.class),
        RULE(Rule.class);

        private final Class classType;

        BaseType(Class classType){
            this.classType = classType;
        }

        public Class getClassType(){
            return classType;
        }
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum ConceptProperty {
        //Unique Properties
        TYPE_LABEL(String.class), INDEX(String.class), ID(String.class),

        //Other Properties
        TYPE(String.class), IS_ABSTRACT(Boolean.class), IS_IMPLICIT(Boolean.class),
        REGEX(String.class), DATA_TYPE(String.class), INSTANCE_COUNT(Long.class),
        RULE_LHS(String.class), RULE_RHS(String.class),

        //Supported Data Types
        VALUE_STRING(String.class), VALUE_LONG(Long.class),
        VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class),
        VALUE_INTEGER(Integer.class), VALUE_FLOAT(Float.class),
        VALUE_DATE(Date.class);

        private final Class dataType;

        ConceptProperty(Class dataType) {
            this.dataType = dataType;
        }

        public Class getDataType() {
            return dataType;
        }
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        ROLE_TYPE_LABEL(String.class),
        RELATION_TYPE_LABEL(String.class),
        REQUIRED(Boolean.class);

        private final Class dataType;

        EdgeProperty(Class dataType) {
            this.dataType = dataType;
        }

        public Class getDataType() {
            return dataType;
        }
    }

    /**
     * This stores the schema which is required when implicitly creating roles for the has-resource methods
     */
    public enum ImplicitType {
        /**
         * The label of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
         */
        HAS("has-%s"),

        /**
         * The label of a role in has-resource, played by the owner of the resource
         */
        HAS_OWNER("has-%s-owner"),

        /**
         * The label of a role in has-resource, played by the resource
         */
        HAS_VALUE("has-%s-value"),

        /**
         * The label of the generic key relationship, used for attaching resources to instances with the 'has' syntax and additionally constraining them to be unique
         */
        KEY("key-%s"),

        /**
         * The label of a role in key, played by the owner of the key
         */
        KEY_OWNER("key-%s-owner"),

        /**
         * The label of a role in key, played by the resource
         */
        KEY_VALUE("key-%s-value");

        private final String label;

        ImplicitType(String label) {
            this.label = label;
        }

        public TypeLabel getLabel(TypeLabel resourceType) {
            return resourceType.map(resource -> String.format(label, resource));
        }

        public TypeLabel getLabel(String resourceType) {
            return TypeLabel.of(String.format(label, resourceType));
        }
    }

    /**
     * An enum representing analytics schema elements
     */
    public enum Analytics {

        DEGREE("degree"),
        CLUSTER("cluster");

        private final String label;

        Analytics(String label) {
            this.label = label;
        }

        public TypeLabel getLabel() {
            return TypeLabel.of(label);
        }
    }

    /**
     *
     * @param typeLabel The resource type label
     * @param value The value of the resource
     * @return A unique id for the resource
     */
    public static String generateResourceIndex(TypeLabel typeLabel, String value){
        return Schema.BaseType.RESOURCE.name() + "-" + typeLabel + "-" + value;
    }
}

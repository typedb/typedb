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

/**
 * A type enum which restricts the types of links/concepts which can be created
 */
public final class Schema {
    private Schema(){
        throw new UnsupportedOperationException();
    }

    /**
     * The different types of edges between vertices
     */
    public enum EdgeLabel {
        ISA("isa"),
        SUB("sub"),
        HAS_ROLE("has-role"),
        PLAYS_ROLE("plays-role"),
        HAS_SCOPE("has-scope"),
        CASTING("casting"),
        ROLE_PLAYER("role-player"),
        HYPOTHESIS("hypothesis"),
        CONCLUSION("conclusion"),
        SHORTCUT("shortcut");

        private final String label;

        EdgeLabel(String l){
            label = l;
        }

        public String getLabel(){
            return label;
        }

        public static EdgeLabel getEdgeLabel(String label){
            for (EdgeLabel edgeLabel : EdgeLabel.values()) {
                if(edgeLabel.getLabel().equals(label)){
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
        TYPE("type"),
        ENTITY_TYPE("entity-type"),
        ROLE_TYPE("role-type"),
        RESOURCE_TYPE("resource-type"),
        RELATION_TYPE("relation-type"),
        RULE_TYPE("rule-type"),
        INFERENCE_RULE("inference-rule"),
        CONSTRAINT_RULE("constraint-rule");


        private final String id;
        MetaSchema(String i){
            id = i;
        }

        public String getId(){
            return id;
        }

        public static boolean isMetaId(String id){
            for (MetaSchema metaSchema : MetaSchema.values()) {
                if(metaSchema.getId().equals(id))
                    return true;
            }
            return false;
        }
    }

    /**
     * Base Types reflecting the possible objects in the concept
     */
    public enum BaseType{
        RELATION, CASTING, TYPE, ROLE_TYPE, RELATION_TYPE, RESOURCE_TYPE, ENTITY, RESOURCE, RULE, RULE_TYPE, ENTITY_TYPE
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum ConceptProperty {
        //Unique Properties
        ITEM_IDENTIFIER(String.class), NAME(String.class), INDEX(String.class),

        //Other Properties
        TYPE(String.class), IS_ABSTRACT(Boolean.class), IS_IMPLICIT(Boolean.class),
        REGEX(String.class), DATA_TYPE(String.class), IS_UNIQUE(Boolean.class),
        IS_MATERIALISED(Boolean.class), IS_EXPECTED(Boolean.class), RULE_LHS(String.class), RULE_RHS(String.class),
        VALUE_STRING(String.class), VALUE_LONG(Long.class), VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class);

        private final Class dataType;
        ConceptProperty(Class dataType){
            this.dataType = dataType;
        }
        public Class getDataType(){
            return dataType;
        }
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        ROLE_TYPE(String.class),
        RELATION_ID(String.class),
        RELATION_TYPE_ID(String.class),
        TO_ID(String.class),
        TO_ROLE(String.class),
        TO_TYPE(String.class),
        FROM_ID(String.class),
        FROM_ROLE(String.class),
        FROM_TYPE(String.class),
        SHORTCUT_HASH(String.class),
        REQUIRED(Boolean.class);

        private final Class dataType;
        EdgeProperty(Class dataType){
            this.dataType = dataType;
        }
        public Class getDataType(){
            return dataType;
        }
    }

    /**
     * This stores the schema which is required when implicitly creating roles for the has-resource methods
     */
    public enum Resource{
        /**
         * The id of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
         */
        HAS_RESOURCE("has-%s"),

        /**
         * The id of a role in has-resource, played by the owner of the resource
         */
        HAS_RESOURCE_OWNER("has-%s-owner"),

        /**
         * The id of a role in has-resource, played by the resource
         */
        HAS_RESOURCE_VALUE("has-%s-value");

        private final String name;

        Resource(String name) {
            this.name = name;
        }

        public String getId(String resourceTypeId) {
            return String.format(name, resourceTypeId);
        }
    }
}

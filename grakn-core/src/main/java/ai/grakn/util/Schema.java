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

import ai.grakn.concept.TypeName;

/**
 * A type enum which restricts the types of links/concepts which can be created
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
        HAS_ROLE("has-role"),
        PLAYS_ROLE("plays-role"),
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


        private final TypeName name;

        MetaSchema(String i) {
            name = TypeName.of(i);
        }

        public TypeName getName() {
            return name;
        }

        public static boolean isMetaName(TypeName name) {
            for (MetaSchema metaSchema : MetaSchema.values()) {
                if (metaSchema.getName().equals(name)) return true;
            }
            return false;
        }
    }

    /**
     * Base Types reflecting the possible objects in the concept
     */
    public enum BaseType {
        RELATION, CASTING, TYPE, ROLE_TYPE, RELATION_TYPE, RESOURCE_TYPE, ENTITY, RESOURCE, RULE, RULE_TYPE, ENTITY_TYPE
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum ConceptProperty {
        //Unique Properties
        NAME(String.class), INDEX(String.class), ID(String.class),

        //Other Properties
        TYPE(String.class), IS_ABSTRACT(Boolean.class), IS_IMPLICIT(Boolean.class),
        REGEX(String.class), DATA_TYPE(String.class), IS_UNIQUE(Boolean.class),
        IS_MATERIALISED(Boolean.class), IS_EXPECTED(Boolean.class), RULE_LHS(String.class), RULE_RHS(String.class),
        VALUE_STRING(String.class), VALUE_LONG(Long.class), VALUE_DOUBLE(Double.class), VALUE_BOOLEAN(Boolean.class);

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
        ROLE_TYPE(String.class),
        RELATION_ID(String.class),
        RELATION_TYPE_NAME(String.class),
        TO_ID(String.class),
        TO_ROLE_NAME(String.class),
        TO_TYPE_NAME(String.class),
        FROM_ID(String.class),
        FROM_ROLE_NAME(String.class),
        FROM_TYPE_NAME(String.class),
        SHORTCUT_HASH(String.class),
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
    public enum Resource {
        /**
         * The name of the generic has-resource relationship, used for attaching resources to instances with the 'has' syntax
         */
        HAS_RESOURCE("has-%s"),

        /**
         * The name of a role in has-resource, played by the owner of the resource
         */
        HAS_RESOURCE_OWNER("has-%s-owner"),

        /**
         * The name of a role in has-resource, played by the resource
         */
        HAS_RESOURCE_VALUE("has-%s-value");

        private final String name;

        Resource(String name) {
            this.name = name;
        }

        public TypeName getName(TypeName resourceType) {
            return TypeName.of(String.format(name, resourceType.getValue()));
        }
    }

    /**
     * An enum representing analytics schema elements
     */
    public enum Analytics {

        DEGREE("degree"),
        CLUSTER("cluster");

        private final String name;

        Analytics(String name) {
            this.name = name;
        }

        public TypeName getName() {
            return TypeName.of(name);
        }
    }
}

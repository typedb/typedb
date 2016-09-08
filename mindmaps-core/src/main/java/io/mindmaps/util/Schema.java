/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.util;

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
        AKO("ako"),
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
    public enum MetaType {
        TYPE("concept-type"),
        ENTITY_TYPE("entity-type"),
        ROLE_TYPE("role-type"),
        RESOURCE_TYPE("resource-type"),
        RELATION_TYPE("relation-type"),
        RULE_TYPE("rule-type"),
        INFERENCE_RULE("inference-rule"),
        CONSTRAINT_RULE("constraint-rule");


        private final String id;
        MetaType(String i){
            id = i;
        }

        public String getId(){
            return id;
        }

        public static boolean isMetaId(String id){
            for (MetaType metaType : MetaType.values()) {
                if(metaType.getId().equals(id))
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
     * A property enum defining the unique mutable properties of the concept. The must be unique properties.
     */
    public enum ConceptPropertyUnique {
        ITEM_IDENTIFIER, INDEX
    }

    /**
     * An enum which defines the non-unique mutable properties of the concept.
     */
    public enum ConceptProperty {
        TYPE, IS_ABSTRACT,
        REGEX, DATA_TYPE, IS_UNIQUE,
        IS_MATERIALISED, IS_EXPECTED, RULE_LHS, RULE_RHS,
        VALUE_STRING, VALUE_LONG, VALUE_DOUBLE, VALUE_BOOLEAN
    }

    /**
     * A property enum defining the possible labels that can go on the edge label.
     */
    public enum EdgeProperty {
        ROLE_TYPE,
        RELATION_ID,
        RELATION_TYPE_ID,
        TO_ID,
        TO_ROLE,
        TO_TYPE,
        FROM_ID,
        FROM_ROLE,
        FROM_TYPE,
        SHORTCUT_HASH
    }
}

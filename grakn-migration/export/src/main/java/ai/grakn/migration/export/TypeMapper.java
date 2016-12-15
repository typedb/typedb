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
package ai.grakn.migration.export;

import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Var;

import static ai.grakn.graql.Graql.var;

/**
 * Map Grakn Core type to equivalent Graql representation
 * @author alexandraorth
 */
public class TypeMapper {

    /**
     * Map a Type to the Graql string representation
     * @param type type to be mapped
     * @return Graql var equivalent to the given type
     */
    public static Var map(Type type) {
        Var mapped = formatBase(type);
        if (type.isEntityType()) {
            mapped = map(mapped, type.asEntityType());
        } else if (type.isRelationType()) {
            mapped = map(mapped, type.asRelationType());
        } else if (type.isRoleType()) {
            mapped = map(mapped, type.asRoleType());
        } else if (type.isResourceType()) {
            mapped = map(mapped, type.asResourceType());
        } else if (type.isRuleType()) {
            mapped = map(mapped, type.asRuleType());
        }

        return mapped;
    }

    /**
     * Map an EntityType to a Var
     * @param var holder var with basic information
     * @param entityType type to be mapped
     * @return var with EntityType specific metadata
     */
    private static Var map(Var var, EntityType entityType) {
        return var;
    }

    /**
     * Map a RelationType to a Var with all of the has-role edges
     * @param var holder var with basic information
     * @param relationType type to be mapped
     * @return var with RelationType specific metadata
     */
    private static Var map(Var var, RelationType relationType) {
        return hasRoles(var, relationType);
    }

    /**
     * Map a RoleType to a Var
     * @param var holder var with basic information
     * @param roleType type to be mapped
     * @return var with RoleType specific metadata
     */
    private static Var map(Var var, RoleType roleType) {
        return var;
    }

    /**
     * Map a ResourceType to a Var with the datatype
     * @param var holder var with basic information
     * @param resourceType type to be mapped
     * @return var with ResourceType specific metadata
     */
    private static Var map(Var var, ResourceType resourceType) {
        return datatype(var, resourceType);
    }

    /**
     * Map a RuleType to a Var
     * @param var holder var with basic information
     * @param ruleType type to be mapped
     * @return var with RuleType specific metadata
     */
    private static Var map(Var var, RuleType ruleType) {
        return var;
    }

    /**
     * Create a var with the information underlying all Types
     * @param type type to be mapped
     * @return Var containing basic information about the given type
     */
    private static Var formatBase(Type type) {
        Var var = var().name(type.getName());

        Type superType = type.superType();
        if (type.superType() != null) {
            var.sub(superType.getName());
        }

        var = playsRoles(var, type);
        var = isAbstract(var, type);

        return var;
    }

    /**
     * Add is-abstract annotation to a var
     * @param var var to be marked
     * @param type type from which metadata extracted
     */
    private static Var isAbstract(Var var, Type type) {
       return type.isAbstract() ? var.isAbstract() : var;
    }

    /**
     * Add plays-role edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate plays-role edges
     */
    private static Var playsRoles(Var var, Type type) {
        for(RoleType role:type.playsRoles()){
            var = var.playsRole(role.getName());
        }
        return var;
    }

    /**
     * Add has-role edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate has-role edges
     */
    private static Var hasRoles(Var var, RelationType type){
        for(RoleType role:type.hasRoles()){
            var = var.hasRole(role.getName());
        }
        return var;
    }

    /**
     * Add a datatype to a resource type var
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate datatype
     */
    private static Var datatype(Var var, ResourceType type) {
        ResourceType.DataType dataType = type.getDataType();
        if (dataType != null) {
            return var.datatype(dataType);
        } else {
            return var;
        }
    }
}

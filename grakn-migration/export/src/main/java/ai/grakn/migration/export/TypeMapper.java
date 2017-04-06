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

import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.graql.Graql;
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
        if (type.isRelationType()) {
            mapped = map(mapped, type.asRelationType());
        } else if (type.isResourceType()) {
            mapped = map(mapped, type.asResourceType());
        }

        return mapped;
    }

    /**
     * Map a RelationType to a Var with all of the relates edges
     * @param var holder var with basic information
     * @param relationType type to be mapped
     * @return var with RelationType specific metadata
     */
    private static Var map(Var var, RelationType relationType) {
        return relates(var, relationType);
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
     * Create a var with the information underlying all Types
     * @param type type to be mapped
     * @return Var containing basic information about the given type
     */
    private static Var formatBase(Type type) {
        Var var = var().label(type.getLabel());

        Type superType = type.superType();
        if (type.superType() != null) {
            var = var.sub(Graql.label(superType.getLabel()));
        }

        var = plays(var, type);
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
     * Add plays edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate plays edges
     */
    private static Var plays(Var var, Type type) {
        for(RoleType role:type.plays()){
            var = var.plays(Graql.label(role.getLabel()));
        }
        return var;
    }

    /**
     * Add relates edges to a var, given a type
     * @param var var to be modified
     * @param type type from which metadata extracted
     * @return var with appropriate relates edges
     */
    private static Var relates(Var var, RelationType type){
        for(RoleType role:type.relates()){
            var = var.relates(Graql.label(role.getLabel()));
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

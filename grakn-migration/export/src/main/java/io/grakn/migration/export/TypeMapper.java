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
package io.grakn.migration.export;

import io.grakn.concept.EntityType;
import io.grakn.concept.RelationType;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.concept.RuleType;
import io.grakn.concept.Type;
import io.grakn.graql.Var;

import static io.grakn.graql.Graql.var;

public class TypeMapper {

    public static String map(Type type) {
        Var mapped = formatBase(type);
        if (type instanceof EntityType) {
            mapped = map(mapped, type.asEntityType());
        } else if (type instanceof RelationType) {
            mapped = map(mapped, type.asRelationType());
        } else if (type instanceof RoleType) {
            mapped = map(mapped, type.asRoleType());
        } else if (type instanceof ResourceType) {
            mapped = map(mapped, type.asResourceType());
        } else if (type instanceof RuleType) {
            mapped = map(mapped, type.asRuleType());
        }

        return mapped.toString();
    }

    public static Var map(Var var, EntityType entityType) {
        return var;
    }

    public static Var map(Var var, RelationType relationType) {
        return hasRoles(var, relationType);
    }

    public static Var map(Var var, RoleType roleType) {
        return var;
    }

    public static Var map(Var var, ResourceType resourceType) {
        return datatype(var, resourceType);
    }

    public static Var map(Var var, RuleType ruleType) {
        return var;
    }

    private static Var formatBase(Type type) {
        Var var = var().id(type.getId()).isa(type.type().getId());
        var = playsRoles(var, type);
        var = isAbstract(var, type);

        return var;
    }

    private static Var isAbstract(Var var, Type type) {
       return type.isAbstract() ? var.isAbstract() : var;
    }

    private static Var playsRoles(Var var, Type type) {
        for(RoleType role:type.playsRoles()){
            var = var.playsRole(role.getId());
        }
        return var;
    }

    private static Var hasRoles(Var var, RelationType type){
        for(RoleType role:type.hasRoles()){
            var = var.hasRole(role.getId());
        }
        return var;
    }

    private static Var datatype(Var var, ResourceType type) {
        return var.datatype(type.getDataType());
    }
}

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

import ai.grakn.concept.Entity;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarPattern;
import ai.grakn.util.CommonUtil;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;

/**
 * Map Grakn Core instance to Graql representation
 * @author alexandraorth
 */
public class InstanceMapper {

    /**
     * Map an Thing to the equivalent Graql representation
     * @param thing thing to be mapped
     * @return Graql representation of given thing
     */
    public static VarPattern map(Thing thing){
        if(thing.isEntity()){
            return map(thing.asEntity());
        } else if(thing.isResource()){
            return map(thing.asResource());
        } else if(thing.isRelationship()){
            return map(thing.asRelationship());
        } else if(thing.isRule()){
            return map(thing.asRule());
        } else {
            throw CommonUtil.unreachableStatement("Unrecognised thing " + thing);
        }
    }

    /**
     * Map a {@link Entity} to a {@link VarPattern}
     * This includes mapping the instance itself, its id and any has relations
     * @param entity entity to be mapped
     * @return var patterns representing given instance
     */
    private static VarPattern map(Entity entity){
        return base(entity);
    }

    /**
     * Map a {@link Relationship} to a var, along with all of the roleplayers
     * Exclude any relations that are mapped to an encountered resource
     * @param relationship {@link Relationship} to be mapped
     * @return var patterns representing the given instance
     */
    //TODO resources on relations
    private static VarPattern map(Relationship relationship){
        if(relationship.type().isImplicit()){
            return var();
        }

        VarPattern var = base(relationship);
        var = roleplayers(var, relationship);
        return var;
    }

    /**
     * Map a Resource to a var IF it is not attached in a has relation to another instance
     * @param resource resource to be mapped
     * @return var patterns representing the given instance
     */
    private static VarPattern map(Resource resource){
        if(isHasResourceResource(resource)){
            return var();
        }

        VarPattern var = base(resource);
        var = var.val(resource.getValue());
        return var;
    }

    /**
     * Map a Rule to a var
     * @param rule rule to be mapped
     * @return var patterns representing the given instance
     */
    //TODO hypothesis, conclusion, isMaterialize, etc
    private static VarPattern map(Rule rule){
        VarPattern var = base(rule);
        var = var.when(and(rule.getWhen()));
        var = var.then(and(rule.getThen()));
        return var;
    }

    /**
     * Add the resources of an entity
     * @param var var representing the entity
     * @param thing thing containing resource information
     * @return var pattern with resources
     */
    private static VarPattern hasResources(VarPattern var, Thing thing){
        for(Resource resource: thing.resources().collect(Collectors.toSet())){
           var = var.has(resource.type().getLabel(), var().val(resource.getValue()));
        }
        return var;
    }

    /**
     * Add the roleplayers of a {@link Relationship} to the relationship var
     * @param var var representing the relationship
     * @param relationship {@link Relationship} that contains roleplayer data
     * @return var pattern with roleplayers
     */
    private static VarPattern roleplayers(VarPattern var, Relationship relationship){
        for(Map.Entry<Role, Set<Thing>> entry: relationship.allRolePlayers().entrySet()){
            Role role = entry.getKey();
            for (Thing thing : entry.getValue()) {
                var = var.rel(Graql.label(role.getLabel()), thing.getId().getValue());
            }
        }
        return var;
    }

    /**
     * Given an thing, return a var with the type.
     * @param thing thing to map
     * @return var patterns representing given thing
     */
    private static VarPattern base(Thing thing){
        VarPattern var = var(thing.getId().getValue()).isa(Graql.label(thing.type().getLabel()));
        return hasResources(var, thing);
    }

    /**
     * Check if the given resource conforms to the has syntax and structural requirements
     * @param resource resource to check
     * @return true if the resource is target of has relation
     */
    private static boolean isHasResourceResource(Resource resource){
        ResourceType resourceType = resource.type();

        // TODO: Make sure this is tested
        boolean plays = resourceType.plays().map(Role::getLabel)
                .allMatch(c -> c.equals(HAS_VALUE.getLabel(resourceType.getLabel())));
        return resource.ownerInstances().findAny().isPresent() && plays;
    }
}

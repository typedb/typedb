/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */
package ai.grakn.migration.export;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarPattern;
import ai.grakn.util.CommonUtil;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        } else if(thing.isAttribute()){
            return map(thing.asAttribute());
        } else if(thing.isRelationship()){
            return map(thing.asRelationship());
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
     * Map a {@link Attribute} to a var IF it is not attached in a has relation to another instance
     * @param attribute {@link Attribute} to be mapped
     * @return var patterns representing the given instance
     */
    private static VarPattern map(Attribute attribute){
        if(isHasResourceResource(attribute)){
            return var();
        }

        VarPattern var = base(attribute);
        var = var.val(attribute.getValue());
        return var;
    }

    /**
     * Add the resources of an entity
     * @param var var representing the entity
     * @param thing thing containing resource information
     * @return var pattern with resources
     */
    private static VarPattern hasResources(VarPattern var, Thing thing){
        for(Attribute attribute : thing.attributes().collect(Collectors.toSet())){
           var = var.has(attribute.type().getLabel(), var().val(attribute.getValue()));
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
     * Check if the given {@link Attribute} conforms to the has syntax and structural requirements
     * @param attribute {@link Attribute} to check
     * @return true if the {@link Attribute} is target of has relation
     */
    private static boolean isHasResourceResource(Attribute attribute){
        AttributeType attributeType = attribute.type();

        // TODO: Make sure this is tested
        boolean plays = attributeType.plays().map(Role::getLabel)
                .allMatch(c -> c.equals(HAS_VALUE.getLabel(attributeType.getLabel())));
        return attribute.ownerInstances().findAny().isPresent() && plays;
    }
}

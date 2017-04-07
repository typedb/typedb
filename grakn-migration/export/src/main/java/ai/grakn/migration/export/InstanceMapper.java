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
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Var;

import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.and;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.Schema.ImplicitType.HAS_VALUE;

/**
 * Map Grakn Core instance to Graql representation
 * @author alexandraorth
 */
public class InstanceMapper {

    /**
     * Map an Instance to the equivalent Graql representation
     * @param instance instance to be mapped
     * @return Graql representation of given instance
     */
    public static Var map(Instance instance){
        Var mapped = var();
        if(instance.isEntity()){
            mapped = map(instance.asEntity());
        } else if(instance.isResource()){
            mapped = map(instance.asResource());
        } else if(instance.isRelation()){
            mapped = map(instance.asRelation());
        } else if(instance.isRule()){
            mapped = map(instance.asRule());
        }

        return mapped;
    }

    /**
     * Map a Entity to a Var
     * This includes mapping the instance itself, its id and any has relations
     * @param entity entity to be mapped
     * @return var patterns representing given instance
     */
    private static Var map(Entity entity){
        return base(entity);
    }

    /**
     * Map a relation to a var, along with all of the roleplayers
     * Exclude any relations that are mapped to an encountered resource
     * @param relation relation to be mapped
     * @return var patterns representing the given instance
     */
    //TODO resources on relations
    private static Var map(Relation relation){
        if(relation.type().isImplicit()){
            return var();
        }

        Var var = base(relation);
        var = roleplayers(var, relation);
        return var;
    }

    /**
     * Map a Resource to a var IF it is not attached in a has relation to another instance
     * @param resource resource to be mapped
     * @return var patterns representing the given instance
     */
    private static Var map(Resource resource){
        if(isHasResourceResource(resource)){
            return var();
        }

        Var var = base(resource);
        var = var.val(resource.getValue());
        return var;
    }

    /**
     * Map a Rule to a var
     * @param rule rule to be mapped
     * @return var patterns representing the given instance
     */
    //TODO hypothesis, conclusion, isMaterialize, etc
    private static Var map(Rule rule){
        Var var = base(rule);
        var = var.lhs(and(rule.getLHS()));
        var = var.rhs(and(rule.getRHS()));
        return var;
    }

    /**
     * Add the resources of an entity
     * @param var var representing the entity
     * @param instance instance containing resource information
     * @return var pattern with resources
     */
    private static Var hasResources(Var var, Instance instance){
        for(Resource resource:instance.resources()){
           var = var.has(resource.type().getLabel(), var().val(resource.getValue()));
        }
        return var;
    }

    /**
     * Add the roleplayers of a relation to the relation var
     * @param var var representing the relation
     * @param relation relation that contains roleplayer data
     * @return var pattern with roleplayers
     */
    private static  Var roleplayers(Var var, Relation relation){
        for(Map.Entry<RoleType, Set<Instance>> entry:relation.allRolePlayers().entrySet()){
            RoleType role = entry.getKey();
            for (Instance instance : entry.getValue()) {
                var = var.rel(Graql.label(role.getLabel()), instance.getId().getValue());
            }
        }
        return var;
    }

    /**
     * Given an instance, return a var with the type.
     * @param instance instance to map
     * @return var patterns representing given instance
     */
    private static  Var base(Instance instance){
        Var var = var(instance.getId().getValue()).isa(Graql.label(instance.type().getLabel()));
        return hasResources(var, instance);
    }

    /**
     * Check if the given resource conforms to the has syntax and structural requirements
     * @param resource resource to check
     * @return true if the resource is target of has relation
     */
    private static boolean isHasResourceResource(Resource resource){
        ResourceType resourceType = resource.type();

        // TODO: Make sure this is tested
        boolean plays = resourceType.plays().stream().map(RoleType::getLabel)
                .allMatch(c -> c.equals(HAS_VALUE.getLabel(resourceType.getLabel())));
        return !resource.ownerInstances().isEmpty() && plays;
    }
}

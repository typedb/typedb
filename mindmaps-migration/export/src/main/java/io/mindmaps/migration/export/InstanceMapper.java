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
package io.mindmaps.migration.export;

import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.graql.Var;

import java.util.Collection;
import java.util.Collections;

import static io.mindmaps.graql.Graql.var;
import static java.util.stream.Collectors.toSet;

/**
 * Map Grakn Core instance to Graql representation
 */
public class InstanceMapper {

    /**
     * Map an Instance to the equivalent Graql representation
     * @param instance instance to be mapped
     * @return Graql representation of given instance
     */
    public static Var map(Instance instance){
        Var mapped = var();
        if(instance instanceof Entity){
            mapped = map(instance.asEntity());
        } else if(instance instanceof Resource){
            mapped = map(instance.asResource());
        } else if(instance instanceof Relation){
            mapped = map(instance.asRelation());
        } else if(instance instanceof Rule){
            mapped = map(instance.asRule());
        }

        return mapped;
    }

    /**
     * Map a Entity to a Var
     * This includes mapping the instance itself, its id and any has-resource relations
     * @param entity entity to be mapped
     * @return var patterns representing given instance
     */
    private static Var map(Entity entity){
        Var var = base(entity);
        return var;
    }

    /**
     * Map a relation to a var, along with all of the roleplayers
     * Exclude any relations that are mapped to an encountered resource
     * @param relation relation to be mapped
     * @return var patterns representing the given instance
     */
    //TODO resources on relations
    private static Var map(Relation relation){
        if(isHasResourceRelation(relation)){
            return var();
        }

        Var var = base(relation);
        var = roleplayers(var, relation);
//        var = hasResources(var, relation);
        return var;
    }

    /**
     * Map a Resource to a var IF it is not attached in a has-resource relation to another instance
     * @param resource resource to be mapped
     * @return var patterns representing the given instance
     */
    private static Var map(Resource resource){
        if(isHasResourceResource(resource)){
            return var();
        }

        Var var = base(resource);
        var = var.value(resource.getValue());
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
        var = var.lhs(rule.getLHS().toString());
        var = var.rhs(rule.getRHS().toString());
        return var;
    }

    /**
     * Add the resources of an entity
     * @param var var representing the entity
     * @param instance instance containing resource information
     * @return var pattern with resources
     */
    private static Var hasResources(Var var, Instance instance){
        Collection<Resource<?>> resources = Collections.EMPTY_SET;
        if(instance instanceof Resource){
            return var;
        } else if(instance instanceof Entity){
            resources = instance.asEntity().resources();
        } else if (instance instanceof Relation){
            resources = instance.asRelation().resources();
        } else if(instance instanceof Rule){
            resources = instance.asRule().resources();
        }

        for(Resource resource:resources){
           var = var.has(resource.type().getId(), resource.getValue());
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
        for(RoleType role:relation.rolePlayers().keySet()){
            var = var.rel(role.getId(), relation.rolePlayers().get(role).getId());
        }
        return var;
    }

    /**
     * Given an instance, return a var with the type.
     * @param instance instance to map
     * @return var patterns representing given instance
     */
    private static  Var base(Instance instance){
        Var var = var(instance.getId()).isa(instance.type().getId());
        return hasResources(var, instance);
    }

    /**
     * Check if the given relation conforms to the has-resource syntax and requirements
     * @param relation relation instance to check
     * @return true if the relation is a has-resource relation
     */
    private static boolean isHasResourceRelation(Relation relation){
        String relationType = relation.type().getId();

        if(!relationType.startsWith("has-")){
            return false;
        }

        Collection<String> roles = relation.rolePlayers().keySet().stream().map(Concept::getId).collect(toSet());

        return  roles.size() == 2 &&
                roles.contains(relationType + "-value") &&
                roles.contains(relationType + "-owner");
    }

    /**
     * Check if the given resource conforms to the has-resource syntax and structural requirements
     * @param resource resource to check
     * @return true if the resource is target of has-resource relation
     */
    private static boolean isHasResourceResource(Resource resource){
        ResourceType resourceType = resource.type();

        boolean playsRole = resourceType.playsRoles().stream().map(Concept::getId)
                .allMatch(c -> c.equals("has-" + resourceType.getId() + "-value"));
        return !resource.ownerInstances().isEmpty() && playsRole;
    }
}

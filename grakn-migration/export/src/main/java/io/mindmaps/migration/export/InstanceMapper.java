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

import static io.mindmaps.graql.Graql.var;
import static java.util.stream.Collectors.toSet;

public class InstanceMapper {

    public static String map(Instance instance){
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

        return mapped.toString().replaceAll(" has-resource$", " \"has-resource\"").replaceAll(" has-resource ", " \"has-resource\" ");
    }

    public static Var map(Entity entity){
        Var var = base(entity);
        var = hasResources(var, entity);
        var = var.id(entity.getId());
        return var;
    }

    /**
     *
     * Exclude any relations that are mapped to an encountered resource
     * @param relation
     * @return
     */
    //TODO resources on relations
    public static Var map(Relation relation){
        if(isHasResourceRelation(relation)){
            return var();
        }

        Var var = base(relation);
        var = roleplayers(var, relation);
//        var = hasResources(var, relation);
        return var;
    }

    /**
     *
     * Exclude any resources that have been encountered while mapping an entity.
     * @param resource
     * @return
     */
    public static Var map(Resource resource){
        if(isHasResourceResource(resource)){
            return var();
        }

        Var var = base(resource);
        var = var.value(resource.getValue());
        return var;
    }

    //TODO hypothesis, conclusion, isMaterialize, etc
    public static Var map(Rule rule){
        Var var = base(rule);
        var = var.id(rule.getId());
        var = var.lhs(rule.getLHS());
        var = var.rhs(rule.getRHS());
        return var;
    }

    private static Var hasResources(Var var, Entity entity){
        for(Resource resource:entity.resources()){
           var = var.has(resource.type().getId(), resource.getValue());
        }
        return var;
    }

    private static  Var roleplayers(Var var, Relation relation){
        for(RoleType role:relation.rolePlayers().keySet()){
            var = var.rel(role.getId(), relation.rolePlayers().get(role).getId());
        }
        return var;
    }

    private static  Var base(Instance instance){
        return var(instance.getId()).isa(instance.type().getId());
    }

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

    private static boolean isHasResourceResource(Resource resource){
        ResourceType resourceType = resource.type();

        boolean playsRole = resourceType.playsRoles().stream().map(Concept::getId)
                .allMatch(c -> c.equals("has-" + resourceType.getId() + "-value"));
        return !resource.ownerInstances().isEmpty() && playsRole;
    }
}

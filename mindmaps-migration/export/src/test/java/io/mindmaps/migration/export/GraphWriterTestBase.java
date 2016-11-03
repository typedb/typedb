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

import io.mindmaps.Mindmaps;
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.Type;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public abstract class GraphWriterTestBase {

    protected MindmapsGraph original;
    protected MindmapsGraph copy;
    protected GraphWriter writer;

    @After
    public void clear(){
        copy.clear();
        original.clear();
    }

    public void assertDataEqual(MindmapsGraph one, MindmapsGraph two){
        one.getMetaType().instances().stream()
                .flatMap(c -> c.asType().instances().stream())
                .map(Concept::asInstance)
                .forEach(i -> assertInstanceCopied(i, two));
    }

    public void assertInstanceCopied(Instance instance, MindmapsGraph two){
        if(instance instanceof Entity){
            assertEntityCopied(instance.asEntity(), two);
        } else if(instance instanceof Relation){
            assertRelationCopied(instance.asRelation(), two);
        } else if(instance instanceof Rule){
            assertRuleCopied(instance.asRule(), two);
        } else if(instance instanceof Resource){
            assertResourceCopied(instance.asResource(), two);
        }
    }

    /**
     * Assert that there are the same number of entities in each graph with the same resources
     */
    public void assertEntityCopied(Entity entity1, MindmapsGraph two){
        Collection<Entity> entitiesFromGraph1 = entity1.resources().stream().map(Resource::ownerInstances).flatMap(Collection::stream).map(Concept::asEntity).collect(toSet());
        Collection<Entity> entitiesFromGraph2 = getInstancesByResources(two, entity1).stream().map(Concept::asEntity).collect(toSet());

        assertEquals(entitiesFromGraph1.size(), entitiesFromGraph2.size());
    }

    /**
     * Get all instances with the same resources
     */
    public Collection<Instance> getInstancesByResources(MindmapsGraph graph, Instance instance){
        Collection<Resource<?>> resources = Collections.EMPTY_SET;
        if(instance instanceof Resource){
            return Collections.singleton(getResourceFromGraph(graph, instance.asResource()));
        } else if(instance instanceof Entity){
            resources = instance.asEntity().resources();
        } else if (instance instanceof Relation){
            resources = instance.asRelation().resources();
        } else if(instance instanceof Rule){
            resources = instance.asRule().resources();
        }

        return resources.stream()
                .map(r -> getResourceFromGraph(graph, r))
                .map(Resource::ownerInstances)
                .flatMap(Collection::stream)
                .collect(toSet());
    }

    /**
     * Get an entity that is uniquely defined by its resources
     */
    public Instance getInstanceUniqueByResourcesFromGraph(MindmapsGraph graph, Instance instance){
        System.out.println(instance);
        System.out.println(getInstancesByResources(graph, instance));
        return getInstancesByResources(graph, instance)
               .iterator().next();
    }

    public <V> Resource<V> getResourceFromGraph(MindmapsGraph graph, Resource<V> resource){
        return graph.getResource(resource.getValue(), resource.type());
    }

    public void assertRelationCopied(Relation relation1, MindmapsGraph two){
        if(relation1.rolePlayers().values().stream().anyMatch(Concept::isResource)){
            return;
        }

        RelationType relationType = two.getRelationType(relation1.type().getId());
        Map<RoleType, Instance> rolemap = relation1.rolePlayers().entrySet().stream().collect(toMap(
                e -> two.getRoleType(e.getKey().getId()),
                e -> getInstanceUniqueByResourcesFromGraph(two, e.getValue())
        ));

        assertNotNull(two.getRelation(relationType, rolemap));
    }

    public void assertResourceCopied(Resource resource1, MindmapsGraph two){
        assertEquals(true, two.getResourcesByValue(resource1.getValue()).stream()
                .map(Concept::type)
                .map(Type::getId)
                .anyMatch(t -> resource1.type().getId().equals(t)));
    }

    public void assertRuleCopied(Rule rule1, MindmapsGraph two){
        Rule rule2 = getInstanceUniqueByResourcesFromGraph(two, rule1).asRule();

        assertEquals(rule1.getLHS(), rule2.getLHS());
        assertEquals(rule1.getRHS(), rule2.getRHS());
    }

    public void assertOntologiesEqual(MindmapsGraph one, MindmapsGraph two){
        boolean ontologyCorrect = one.getMetaType().instances().stream()
                .allMatch(t -> typesEqual(t.asType(), two.getType(t.getId())));
        assertEquals(true, ontologyCorrect);
    }

    public boolean typesEqual(Type one, Type two){
        return one.getId().equals(two.getId())
                && one.isAbstract().equals(two.isAbstract())
                && one.type().getId().equals(two.type().getId())
                && (!one.isResourceType() || one.asResourceType().getDataType().equals(two.asResourceType().getDataType()));
    }
}

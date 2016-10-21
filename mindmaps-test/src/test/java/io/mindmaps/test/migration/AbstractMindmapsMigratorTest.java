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

package io.mindmaps.test.migration;

import com.google.common.io.Files;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.graql.Graql;
import io.mindmaps.graql.internal.util.GraqlType;
import io.mindmaps.test.AbstractMindmapsEngineTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static io.mindmaps.concept.ResourceType.DataType;

public class AbstractMindmapsMigratorTest extends AbstractMindmapsEngineTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @After
    public void clear(){
        graph.clear();
    }

    public static File getFile(String component, String fileName){
        return new File(AbstractMindmapsMigratorTest.class.getResource(component + "/" + fileName).getPath());
    }

    public static void load(File ontology) {
        try {
            Graql.withGraph(graph)
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException |MindmapsValidationException e){
            throw new RuntimeException(e);
        }
    }

    public static ResourceType assertResourceTypeExists(String name, DataType datatype) {
        ResourceType resourceType = graph.getResourceType(name);
        assertNotNull(resourceType);
        assertEquals(datatype.getName(), resourceType.getDataType().getName());
        return resourceType;
    }

    public static void assertResourceTypeRelationExists(String name, DataType datatype, Type owner){
        ResourceType resource = assertResourceTypeExists(name, datatype);

        RelationType relationType = graph.getRelationType(GraqlType.HAS_RESOURCE.getId(name));
        RoleType roleOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(name));
        RoleType roleOther = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(name));

        assertNotNull(relationType);
        assertNotNull(roleOwner);
        assertNotNull(roleOther);

        assertEquals(relationType, roleOwner.relationType());
        assertEquals(relationType, roleOther.relationType());

        assertTrue(owner.playsRoles().contains(roleOwner));
        assertTrue(resource.playsRoles().contains(roleOther));
    }

    public static void assertResourceEntityRelationExists(String resourceName, Object resourceValue, Entity owner){
        ResourceType resourceType = graph.getResourceType(resourceName);

        assertEquals(resourceValue, owner.resources(resourceType).stream()
                .map(Resource::getValue)
                .findFirst().get());
    }

    public static void assertRelationBetweenTypesExists(Type type1, Type type2, String relation){
        RelationType relationType = graph.getRelationType(relation);

        RoleType role1 = type1.playsRoles().stream().filter(r -> r.relationType().getId().equals(relation)).findFirst().get();
        RoleType role2 = type2.playsRoles().stream().filter(r -> r.relationType().getId().equals(relation)).findFirst().get();

        assertTrue(relationType.hasRoles().contains(role1));
        assertTrue(relationType.hasRoles().contains(role2));
    }

    public static void assertRelationBetweenInstancesExists(Instance instance1, Instance instance2, String relation){
        RelationType relationType = graph.getRelationType(relation);

        RoleType role1 = instance1.playsRoles().stream().filter(r -> r.relationType().equals(relationType)).findFirst().get();
        assertTrue(instance1.relations(role1).stream().anyMatch(rel -> rel.rolePlayers().values().contains(instance2)));
    }


    public static Instance getProperty(Instance instance, String name) {
        assertEquals(getProperties(instance, name).size(), 1);
        return getProperties(instance, name).iterator().next();
    }

    public static Collection<Instance> getProperties(Instance instance, String name) {
        RelationType relation = graph.getRelationType(name);

        Set<Instance> instances = new HashSet<>();

        relation.instances().stream()
                .filter(i -> i.rolePlayers().values().contains(instance))
                .forEach(i -> instances.addAll(i.rolePlayers().values()));

        instances.remove(instance);
        return instances;
    }

    public static Resource getResource(Instance instance, String name) {
        assertEquals(getResources(instance, name).count(), 1);
        return getResources(instance, name).findAny().get();
    }

    public static Stream<Resource> getResources(Instance instance, String name) {
        RoleType roleOwner = graph.getRoleType(GraqlType.HAS_RESOURCE_OWNER.getId(name));
        RoleType roleOther = graph.getRoleType(GraqlType.HAS_RESOURCE_VALUE.getId(name));

        Collection<Relation> relations = instance.relations(roleOwner);
        return relations.stream().map(r -> r.rolePlayers().get(roleOther).asResource());
    }
}

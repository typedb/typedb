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

package ai.grakn.test.migration;

import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeName;
import ai.grakn.engine.GraknEngineServer;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.migration.base.Migrator;
import ai.grakn.migration.base.io.MigrationLoader;
import ai.grakn.util.Schema;
import com.google.common.io.Files;

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

public class MigratorTestUtils {

    public static String getFileAsString(String component, String fileName){
        try {
            return Files.readLines(getFile(component, fileName), StandardCharsets.UTF_8).stream().collect(joining());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static File getFile(String component, String fileName){
        return new File(MigratorTestUtils.class.getResource(component + "/" + fileName).getPath());
    }

    public static void migrate(GraknGraph graph, Migrator migrator){
        MigrationLoader.load(GraknEngineServer.getClusterManager(), graph.getKeyspace(), migrator);
    }

    public static void load(GraknGraph graph, File ontology) {
        try {
            graph.graql()
                    .parse(Files.readLines(ontology, StandardCharsets.UTF_8).stream().collect(joining("\n")))
                    .execute();

            graph.commit();
        } catch (IOException |GraknValidationException e){
            throw new RuntimeException(e);
        }
    }

    public static void assertResourceEntityRelationExists(GraknGraph graph, String resourceName, Object resourceValue, Entity owner){
        ResourceType resourceType = graph.getResourceType(resourceName);
        assertNotNull(resourceType);
        assertEquals(resourceValue, owner.resources(resourceType).stream()
                .map(Resource::getValue)
                .findFirst().get());
    }

    public static void assertRelationBetweenInstancesExists(GraknGraph graph, Instance instance1, Instance instance2, TypeName relation){
        RelationType relationType = graph.getType(relation);

        RoleType role1 = instance1.playsRoles().stream().filter(r -> r.relationTypes().stream().anyMatch(rel -> rel.equals(relationType))).findFirst().get();
        assertTrue(instance1.relations(role1).stream().anyMatch(rel -> rel.rolePlayers().values().contains(instance2)));
    }


    public static Instance getProperty(GraknGraph graph, Instance instance, String name) {
        assertEquals(getProperties(graph, instance, name).size(), 1);
        return getProperties(graph, instance, name).iterator().next();
    }

    public static Collection<Instance> getProperties(GraknGraph graph, Instance instance, String name) {
        RelationType relation = graph.getRelationType(name);

        Set<Instance> instances = new HashSet<>();

        relation.instances().stream()
                .filter(i -> i.rolePlayers().values().contains(instance))
                .forEach(i -> instances.addAll(i.rolePlayers().values()));

        instances.remove(instance);
        return instances;
    }

    public static Resource getResource(GraknGraph graph, Instance instance, TypeName name) {
        assertEquals(getResources(graph, instance, name).count(), 1);
        return getResources(graph, instance, name).findAny().get();
    }

    public static Stream<Resource> getResources(GraknGraph graph, Instance instance, TypeName name) {
        RoleType roleOwner = graph.getType(Schema.Resource.HAS_RESOURCE_OWNER.getName(name));
        RoleType roleOther = graph.getType(Schema.Resource.HAS_RESOURCE_VALUE.getName(name));

        Collection<Relation> relations = instance.relations(roleOwner);
        return relations.stream().map(r -> r.rolePlayers().get(roleOther).asResource());
    }

    /**
     * Check that the pet graph has been loaded correctly
     */
    public static void assertPetGraphCorrect(GraknGraph graph){
        Collection<Entity> pets = graph.getEntityType("pet").instances();
        assertEquals(9, pets.size());

        Collection<Entity> cats = graph.getEntityType("cat").instances();
        assertEquals(2, cats.size());

        Collection<Entity> hamsters = graph.getEntityType("hamster").instances();
        assertEquals(1, hamsters.size());

        ResourceType<String> name = graph.getResourceType("name");
        ResourceType<String> death = graph.getResourceType("death");

        Entity puffball = name.getResource("Puffball").ownerInstances().iterator().next().asEntity();
        assertEquals(0, puffball.resources(death).size());

        Entity bowser = name.getResource("Bowser").ownerInstances().iterator().next().asEntity();
        assertEquals(1, bowser.resources(death).size());
    }

    /**
     * Check that the pokemon graph has been loaded correctly
     */
    public static void assertPokemonGraphCorrect(GraknGraph graph){
        Collection<Entity> pokemon = graph.getEntityType("pokemon").instances();
        assertEquals(9, pokemon.size());

        ResourceType<String> typeid = graph.getResourceType("type-id");
        ResourceType<String> pokedexno = graph.getResourceType("pokedex-no");

        Entity grass = typeid.getResource("12").ownerInstances().iterator().next().asEntity();
        Entity poison = typeid.getResource("4").ownerInstances().iterator().next().asEntity();
        Entity bulbasaur = pokedexno.getResource("1").ownerInstances().iterator().next().asEntity();
        RelationType relation = graph.getRelationType("has-type");

        assertNotNull(grass);
        assertNotNull(poison);
        assertNotNull(bulbasaur);

        assertRelationBetweenInstancesExists(graph, bulbasaur, grass, relation.getName());
        assertRelationBetweenInstancesExists(graph, bulbasaur, poison, relation.getName());
    }
}

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

package io.grakn.example;

import io.grakn.Grakn;
import io.grakn.GraknGraph;
import io.grakn.concept.Entity;
import io.grakn.concept.Relation;
import io.grakn.concept.RelationType;
import io.grakn.concept.Resource;
import io.grakn.concept.ResourceType;
import io.grakn.concept.RoleType;
import io.grakn.util.ErrorMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;

public class PokemonGraphFactoryTest {
    private GraknGraph graknGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        graknGraph = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        PokemonGraphFactory.loadGraph(graknGraph);
    }

    @Test
    public void failToLoad(){
        GraknGraph graknGraph = Grakn.factory(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a")).getGraph();
        graknGraph.putRelationType("fake");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage())
        ));

        PokemonGraphFactory.loadGraph(graknGraph);
    }

    @Test(expected=InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<PokemonGraphFactory> c = PokemonGraphFactory.class.getDeclaredConstructor();
        c.setAccessible(true);
        c.newInstance();
    }

    @Test
    public void testGraphExists() {
        assertNotNull(graknGraph);
    }

    @Test
    public void testGraphHasPokemon() {
        Entity bulbasaur = graknGraph.getResourcesByValue("Bulbasaur").iterator().next().ownerInstances().iterator().next().asEntity();
        assertTrue(bulbasaur.type().equals(graknGraph.getEntityType("pokemon")));
    }

    @Test
    public void testGraphHasPokemonType() {
        Entity poison = graknGraph.getResourcesByValue("poison").iterator().next().ownerInstances().iterator().next().asEntity();
        assertTrue(poison.type().equals(graknGraph.getEntityType("pokemon-type")));
    }

    @Test
    public void testBulbasaurHasResource() {
        ResourceType<Long> pokedexNo = graknGraph.getResourceType("pokedex-no");
        Entity bulbasaur = graknGraph.getResourcesByValue("Bulbasaur").iterator().next().ownerInstances().iterator().next().asEntity();
        Stream<Resource<?>> resources = bulbasaur.resources().stream();
        assertTrue(resources.anyMatch(r -> r.type().equals(pokedexNo) && r.getValue().equals(1L)));
    }

    @Test
    public void testTypeIsSuperEffective() {
        RelationType relationType = graknGraph.getRelationType("super-effective");
        RoleType role = graknGraph.getRoleType("defending-type");
        Entity poison = graknGraph.getResourcesByValue("poison").iterator().next().ownerInstances().iterator().next().asEntity();
        Entity grass = graknGraph.getResourcesByValue("grass").iterator().next().ownerInstances().iterator().next().asEntity();
        Stream<Relation> relations = poison.relations().stream();
        assertTrue(relations.anyMatch(r -> r.type().equals(relationType)
                && r.rolePlayers().get(role).equals(grass)));
    }

}
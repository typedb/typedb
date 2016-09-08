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

package io.mindmaps.example;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.util.ErrorMessage;
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.Relation;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;

public class PokemonGraphFactoryTest {
    private MindmapsGraph mindmapsGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        PokemonGraphFactory.loadGraph(mindmapsGraph);
    }

    @Test
    public void failToLoad(){
        MindmapsGraph mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        mindmapsGraph.putRelationType("fake");

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage(allOf(
                containsString(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage())
        ));

        PokemonGraphFactory.loadGraph(mindmapsGraph);
    }

    @Test(expected=InvocationTargetException.class)
    public void testConstructorIsPrivate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<PokemonGraphFactory> c = PokemonGraphFactory.class.getDeclaredConstructor();
        c.setAccessible(true);
        c.newInstance();
    }

    @Test
    public void testGraphExists() {
        assertNotNull(mindmapsGraph);
    }

    @Test
    public void testGraphHasPokemon() {
        Entity bulbasaur = mindmapsGraph.getEntity("Bulbasaur");
        assertTrue(bulbasaur.type().equals(mindmapsGraph.getEntityType("pokemon")));
    }

    @Test
    public void testGraphHasPokemonType() {
        Entity poison = mindmapsGraph.getEntity("poison");
        assertTrue(poison.type().equals(mindmapsGraph.getEntityType("pokemon-type")));
    }

    @Test
    public void testBulbasaurHasResource() {
        ResourceType<Long> pokedexNo = mindmapsGraph.getResourceType("pokedex-no");
        Entity weedle = mindmapsGraph.getEntity("Bulbasaur");
        Stream<Resource<?>> resources = weedle.resources().stream();
        assertTrue(resources.anyMatch(r -> r.type().equals(pokedexNo) && r.getValue().equals(1L)));
    }

    @Test
    public void testTypeIsSuperEffective() {
        RelationType relationType = mindmapsGraph.getRelationType("super-effective");
        RoleType role = mindmapsGraph.getRoleType("defending-type");
        Entity poison = mindmapsGraph.getEntity("poison");
        Entity grass = mindmapsGraph.getEntity("grass");
        Stream<Relation> relations = poison.relations().stream();
        assertTrue(relations.anyMatch(r -> r.type().equals(relationType)
                && r.rolePlayers().get(role).equals(grass)));
    }

}
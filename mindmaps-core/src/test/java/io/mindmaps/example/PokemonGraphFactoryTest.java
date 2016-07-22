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

import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.core.model.*;
import io.mindmaps.factory.MindmapsTestGraphFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;

public class PokemonGraphFactoryTest {
    private MindmapsGraph mindmapsGraph;
    private MindmapsTransaction transaction;

    @Before
    public void setup(){
        mindmapsGraph = MindmapsTestGraphFactory.newEmptyGraph();
        PokemonGraphFactory.loadGraph(mindmapsGraph);
        transaction = mindmapsGraph.newTransaction();
    }

    @Test
    public void testGraphExists() {
        assertNotNull(transaction);
    }

    @Test
    public void testGraphHasPokemon() {
        Entity bulbasaur = transaction.getEntity("Bulbasaur");
        assertTrue(bulbasaur.type().equals(transaction.getEntityType("pokemon")));
    }

    @Test
    public void testGraphHasPokemonType() {
        Entity poison = transaction.getEntity("poison");
        assertTrue(poison.type().equals(transaction.getEntityType("pokemon-type")));
    }

    @Test
    public void testBulbasaurHasResource() {
        ResourceType<Long> pokedexNo = transaction.getResourceType("pokedex-no");
        Entity weedle = transaction.getEntity("Bulbasaur");
        Stream<Resource<?>> resources = weedle.resources().stream();
        assertTrue(resources.anyMatch(r -> r.type().equals(pokedexNo) && r.getValue().equals(1L)));
    }

    @Test
    public void testTypeIsSuperEffective() {
        RelationType relationType = transaction.getRelationType("super-effective");
        RoleType role = transaction.getRoleType("defending-type");
        Entity poison = transaction.getEntity("poison");
        Entity grass = transaction.getEntity("grass");
        Stream<Relation> relations = poison.relations().stream();
        assertTrue(relations.anyMatch(r -> r.type().equals(relationType)
                && r.rolePlayers().get(role).equals(grass)));
    }

}
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

package ai.grakn.example;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class PokemonGraphFactoryTest {
    private GraknGraph graknGraph;

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupPokemonGraph() {
        GraknSession factory = Grakn.session(Grakn.IN_MEMORY, UUID.randomUUID().toString().replaceAll("-", "a"));
        graknGraph = factory.open(GraknTxType.WRITE);
        PokemonGraphFactory.loadGraph(graknGraph);
        graknGraph.commit();
        graknGraph = factory.open(GraknTxType.WRITE);
    }

    @After
    public void closeGraph(){
        graknGraph.close();
    }

    @Test
    public void whenQueryingPokemonByName_ReturnPokemon() {
        Entity bulbasaur = graknGraph.getResourcesByValue("Bulbasaur").iterator().next().ownerInstances().iterator().next().asEntity();
        assertEquals(bulbasaur.type(), graknGraph.getEntityType("pokemon"));
    }

    @Test
    public void whenQueryingPokemonTypeByName_ReturnPokemonType() {
        Entity poison = graknGraph.getResourcesByValue("poison").iterator().next().ownerInstances().iterator().next().asEntity();
        assertEquals(poison.type(), graknGraph.getEntityType("pokemon-type"));
    }

    @Test
    public void whenQueryingResourceOfPokemonForSpecificResourceType_ReturnResource() {
        ResourceType<Long> pokedexNo = graknGraph.getResourceType("pokedex-no");
        Instance bulbasaur = graknGraph.getResourcesByValue("Bulbasaur").iterator().next().owner();
        Stream<Resource<?>> resources = bulbasaur.resources().stream();
        assertTrue("Pokemon [" + bulbasaur + "] does not have the resource type [" + pokedexNo + "]", resources.anyMatch(r -> r.type().equals(pokedexNo) && r.getValue().equals(1L)));
    }

    @Test
    public void checkThatPokemonTypeIsEffectiveAgainstAnotherPokeminTypeViaRelation() {
        RelationType relationType = graknGraph.getRelationType("super-effective");
        RoleType role = graknGraph.getRoleType("defending-type");
        Entity poison = graknGraph.getResourcesByValue("poison").iterator().next().ownerInstances().iterator().next().asEntity();
        Entity grass = graknGraph.getResourcesByValue("grass").iterator().next().ownerInstances().iterator().next().asEntity();
        Stream<Relation> relations = poison.relations().stream();
        assertTrue(relations.anyMatch(r -> {
            Collection<Instance> instances = r.rolePlayers(role);
            return r.type().equals(relationType) && !instances.isEmpty() && instances.stream().anyMatch(instance -> instance.equals(grass));
        }));
    }

}
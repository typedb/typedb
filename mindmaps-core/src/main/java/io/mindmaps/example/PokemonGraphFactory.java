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
import io.mindmaps.constants.ErrorMessage;
import io.mindmaps.core.Data;
import io.mindmaps.core.implementation.exception.MindmapsValidationException;
import io.mindmaps.core.model.Entity;
import io.mindmaps.core.model.EntityType;
import io.mindmaps.core.model.RelationType;
import io.mindmaps.core.model.Resource;
import io.mindmaps.core.model.ResourceType;
import io.mindmaps.core.model.RoleType;

/**
 * A class which loads sample data into a mindmaps graph
 */
public class PokemonGraphFactory{
    private static EntityType pokemon;
    private static EntityType pokemonType;
    private static ResourceType<Long> pokedexNo;
    private static ResourceType<String> description;
    private static ResourceType<Long> height;
    private static ResourceType<Long> weight;
    private static RoleType hasResourceTarget;
    private static RoleType hasResourceValue;
    private static RoleType ancestor;
    private static RoleType descendent;
    private static RoleType pokemonWithType;
    private static RoleType typeOfPokemon;
    private static RoleType defendingType;
    private static RoleType attackingType;
    private static RelationType hasResource;
    private static RelationType evolution;
    private static RelationType hasType;
    private static RelationType superEffective;

    private PokemonGraphFactory(){
        throw new UnsupportedOperationException();
    }

    public static void loadGraph(MindmapsGraph mindmapsGraph) {
        buildGraph(mindmapsGraph);
        try {
            mindmapsGraph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage(), e);
        }
    }

    private static void buildGraph(MindmapsGraph graph) {
        buildOntology(graph);
        buildRelations(graph);
        buildInstances(graph);
    }

    private static void buildOntology(MindmapsGraph graph) {

        hasResourceTarget = graph.putRoleType("has-resource-target");
        hasResourceValue = graph.putRoleType("has-resource-value");
        hasResource = graph.putRelationType("has-resource")
                .hasRole(hasResourceTarget)
                .hasRole(hasResourceValue);

        ancestor = graph.putRoleType("ancestor");
        descendent = graph.putRoleType("descendent");
        evolution = graph.putRelationType("evolution")
                .hasRole(ancestor)
                .hasRole(descendent);

        pokemonWithType = graph.putRoleType("pokemon-with-type");
        typeOfPokemon = graph.putRoleType("type-of-pokemon");
        hasType = graph.putRelationType("has-type")
                .hasRole(pokemonWithType)
                .hasRole(typeOfPokemon);

        defendingType = graph.putRoleType("defending-type");
        attackingType = graph.putRoleType("attacking-type");
        superEffective = graph.putRelationType("super-effective")
                .hasRole(defendingType)
                .hasRole(attackingType);

        pokemon = graph.putEntityType("pokemon")
                .playsRole(hasResourceTarget)
                .playsRole(ancestor)
                .playsRole(descendent)
                .playsRole(pokemonWithType);

        pokemonType = graph.putEntityType("pokemon-type")
                .playsRole(typeOfPokemon)
                .playsRole(defendingType)
                .playsRole(attackingType);

        pokedexNo = graph.putResourceType("pokedex-no", Data.LONG)
                .playsRole(hasResourceValue);
        description = graph.putResourceType("description", Data.STRING)
                .playsRole(hasResourceValue);
        height = graph.putResourceType("height", Data.LONG)
                .playsRole(hasResourceValue);
        weight = graph.putResourceType("weight", Data.LONG)
                .playsRole(hasResourceValue);
    }

    private static void buildInstances(MindmapsGraph graph) {
        Entity bulbasaur = graph.putEntity("Bulbasaur", pokemon);
        addResource(graph,bulbasaur,1L,pokedexNo);
        addResource(graph,bulbasaur,"A strange seed was planted on its back at birth. The plant sprouts and grows with this POKéMON.",description);
        addResource(graph,bulbasaur,7L,height);
        addResource(graph,bulbasaur,69L,weight);
        putTypes(graph,bulbasaur,
                graph.getEntity("poison"),graph.getEntity("grass"));

        Entity ivysaur = graph.putEntity("Ivysaur", pokemon);
        addResource(graph,ivysaur,2L,pokedexNo);
        addResource(graph,ivysaur,"When the bulb on its back grows large, it appears to lose the ability to stand on its hind legs.",description);
        addResource(graph,ivysaur,10L,height);
        addResource(graph,ivysaur,130L,weight);
        putTypes(graph,ivysaur,
                graph.getEntity("poison"),graph.getEntity("grass"));
        graph.addRelation(evolution)
                .putRolePlayer(descendent,ivysaur)
                .putRolePlayer(ancestor,bulbasaur);

        Entity venusaur = graph.putEntity("Venusaur", pokemon);
        addResource(graph,venusaur,3L,pokedexNo);
        addResource(graph,venusaur,"The plant blooms when it is absorbing solar energy. It stays on the move to seek sunlight.",description);
        addResource(graph,venusaur,20L,height);
        addResource(graph,venusaur,1000L,weight);
        putTypes(graph,venusaur,
                graph.getEntity("poison"),graph.getEntity("grass"));
        graph.addRelation(evolution)
                .putRolePlayer(descendent,venusaur)
                .putRolePlayer(ancestor,ivysaur);

        Entity charmander = graph.putEntity("Charmander", pokemon);
        addResource(graph,charmander,4L,pokedexNo);
        addResource(graph,charmander,"Obviously prefers hot places. When it rains, steam is said to spout from the tip of its tail.",description);
        addResource(graph,charmander,6L,height);
        addResource(graph,charmander,85L,weight);
        putTypes(graph,charmander,
                graph.getEntity("fire"));

        Entity charmeleon = graph.putEntity("Charmeleon", pokemon);
        addResource(graph,charmeleon,5L,pokedexNo);
        addResource(graph,charmeleon,"When it swings its burning tail, it elevates the temperature to unbearably high levels.",description);
        addResource(graph,charmeleon,11L,height);
        addResource(graph,charmeleon,190L,weight);
        putTypes(graph,charmeleon,
                graph.getEntity("fire"));
        graph.addRelation(evolution)
                .putRolePlayer(descendent,charmeleon)
                .putRolePlayer(ancestor, charmander);

        Entity charizard = graph.putEntity("Charizard", pokemon);
        addResource(graph,charizard,6L,pokedexNo);
        addResource(graph,charizard,"Spits fire that is hot enough to melt boulders. Known to cause forest fires unintentionally.",description);
        addResource(graph,charizard,17L,height);
        addResource(graph,charizard,905L,weight);
        putTypes(graph,charizard,
                graph.getEntity("fire"),graph.getEntity("flying"));
        graph.addRelation(evolution)
                .putRolePlayer(descendent,charizard)
                .putRolePlayer(ancestor, charmeleon);
    }

    private static void addResource(MindmapsGraph graph, Entity pokemon, String s, ResourceType<String> type) {
            Resource<String> resource = graph.putResource(s, type);
        graph.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, pokemon)
                .putRolePlayer(hasResourceValue, resource);
    }

    private static void addResource(MindmapsGraph graph, Entity pokemon, Long l, ResourceType<Long> type) {
            Resource<Long> resource = graph.putResource(l, type);
        graph.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, pokemon)
                .putRolePlayer(hasResourceValue, resource);
    }

    private static void putTypes(MindmapsGraph graph, Entity pokemon, Entity... entities) {
        for (Entity entity : entities) {
            graph.addRelation(hasType)
                    .putRolePlayer(pokemonWithType,pokemon)
                    .putRolePlayer(typeOfPokemon,entity);
        }
    }

    private static void buildRelations(MindmapsGraph graph) {
        Entity normal = graph.putEntity("normal",pokemonType);
        Entity fighting = graph.putEntity("fighting",pokemonType);
        Entity flying = graph.putEntity("flying",pokemonType);
        Entity poison = graph.putEntity("poison",pokemonType);
        Entity ground = graph.putEntity("ground",pokemonType);
        Entity rock = graph.putEntity("rock",pokemonType);
        Entity bug = graph.putEntity("bug",pokemonType);
        Entity ghost = graph.putEntity("ghost",pokemonType);
        Entity steel = graph.putEntity("steel",pokemonType);
        Entity fire = graph.putEntity("fire",pokemonType);
        Entity water = graph.putEntity("water",pokemonType);
        Entity grass = graph.putEntity("grass",pokemonType);
        Entity electric = graph.putEntity("electric",pokemonType);
        Entity psychic = graph.putEntity("psychic",pokemonType);
        Entity ice = graph.putEntity("ice",pokemonType);
        Entity dragon = graph.putEntity("dragon",pokemonType);
        Entity dark = graph.putEntity("dark",pokemonType);
        Entity fairy = graph.putEntity("fairy",pokemonType);
        Entity unknown = graph.putEntity("unknown",pokemonType);
        Entity shadow = graph.putEntity("shadow",pokemonType);

        putSuper(graph,normal,fighting);
        putSuper(graph,rock,fighting);
        putSuper(graph,fighting,flying);
        putSuper(graph,bug,flying);
        putSuper(graph,grass,flying);
        putSuper(graph,grass,poison);
        putSuper(graph,grass,bug);
        putSuper(graph,grass,fire);
        putSuper(graph,fairy,poison);
        putSuper(graph,poison,ground);
        putSuper(graph,ice,steel);
        putSuper(graph,poison,psychic);
        putSuper(graph,ground,grass);
        putSuper(graph,rock,grass);
        putSuper(graph,water,grass);
        putSuper(graph,grass,ice);
    }

    private static void putSuper(MindmapsGraph graph, Entity defend, Entity attack) {
        graph.addRelation(superEffective)
                .putRolePlayer(defendingType,defend)
                .putRolePlayer(attackingType,attack);
    }
}
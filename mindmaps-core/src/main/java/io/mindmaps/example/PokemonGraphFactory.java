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
import io.mindmaps.core.exceptions.MindmapsValidationException;
import io.mindmaps.core.implementation.Data;
import io.mindmaps.core.model.*;

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

    public static void loadGraph(MindmapsGraph mindmapsGraph) {
        MindmapsTransaction transaction = mindmapsGraph.newTransaction();
        buildGraph(transaction);

        try {
            transaction.commit();
        } catch (MindmapsValidationException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void buildGraph(MindmapsTransaction transaction) {
        buildOntology(transaction);
        buildRelations(transaction);
        buildInstances(transaction);
    }

    private static void buildOntology(MindmapsTransaction transaction) {

        hasResourceTarget = transaction.putRoleType("has-resource-target");
        hasResourceValue = transaction.putRoleType("has-resource-value");
        hasResource = transaction.putRelationType("has-resource")
                .hasRole(hasResourceTarget)
                .hasRole(hasResourceValue);

        ancestor = transaction.putRoleType("ancestor");
        descendent = transaction.putRoleType("descendent");
        evolution = transaction.putRelationType("evolution")
                .hasRole(ancestor)
                .hasRole(descendent);

        pokemonWithType = transaction.putRoleType("pokemon-with-type");
        typeOfPokemon = transaction.putRoleType("type-of-pokemon");
        hasType = transaction.putRelationType("has-type")
                .hasRole(pokemonWithType)
                .hasRole(typeOfPokemon);

        defendingType = transaction.putRoleType("defending-type");
        attackingType = transaction.putRoleType("attacking-type");
        superEffective = transaction.putRelationType("super-effective")
                .hasRole(defendingType)
                .hasRole(attackingType);

        pokemon = transaction.putEntityType("pokemon")
                .playsRole(hasResourceTarget)
                .playsRole(ancestor)
                .playsRole(descendent)
                .playsRole(pokemonWithType);

        pokemonType = transaction.putEntityType("pokemon-type")
                .playsRole(typeOfPokemon)
                .playsRole(defendingType)
                .playsRole(attackingType);

        pokedexNo = transaction.putResourceType("pokedex-no", Data.LONG)
                .playsRole(hasResourceValue);
        description = transaction.putResourceType("description", Data.STRING)
                .playsRole(hasResourceValue);
        height = transaction.putResourceType("height", Data.LONG)
                .playsRole(hasResourceValue);
        weight = transaction.putResourceType("weight", Data.LONG)
                .playsRole(hasResourceValue);
    }

    private static void buildInstances(MindmapsTransaction transaction) {
        Entity bulbasaur = transaction.putEntity("Bulbasaur", pokemon);
        addResource(transaction,bulbasaur,1L,pokedexNo);
        addResource(transaction,bulbasaur,"A strange seed was planted on its back at birth. The plant sprouts and grows with this POKéMON.",description);
        addResource(transaction,bulbasaur,7L,height);
        addResource(transaction,bulbasaur,69L,weight);
        putTypes(transaction,bulbasaur,
                transaction.getEntity("poison"),transaction.getEntity("grass"));

        Entity ivysaur = transaction.putEntity("Ivysaur", pokemon);
        addResource(transaction,ivysaur,2L,pokedexNo);
        addResource(transaction,ivysaur,"When the bulb on its back grows large, it appears to lose the ability to stand on its hind legs.",description);
        addResource(transaction,ivysaur,10L,height);
        addResource(transaction,ivysaur,130L,weight);
        putTypes(transaction,ivysaur,
                transaction.getEntity("poison"),transaction.getEntity("grass"));
        transaction.addRelation(evolution)
                .putRolePlayer(descendent,ivysaur)
                .putRolePlayer(ancestor,bulbasaur);

        Entity venusaur = transaction.putEntity("Venusaur", pokemon);
        addResource(transaction,venusaur,3L,pokedexNo);
        addResource(transaction,venusaur,"The plant blooms when it is absorbing solar energy. It stays on the move to seek sunlight.",description);
        addResource(transaction,venusaur,20L,height);
        addResource(transaction,venusaur,1000L,weight);
        putTypes(transaction,venusaur,
                transaction.getEntity("poison"),transaction.getEntity("grass"));
        transaction.addRelation(evolution)
                .putRolePlayer(descendent,venusaur)
                .putRolePlayer(ancestor,ivysaur);

        Entity charmander = transaction.putEntity("Charmander", pokemon);
        addResource(transaction,charmander,4L,pokedexNo);
        addResource(transaction,charmander,"Obviously prefers hot places. When it rains, steam is said to spout from the tip of its tail.",description);
        addResource(transaction,charmander,6L,height);
        addResource(transaction,charmander,85L,weight);
        putTypes(transaction,charmander,
                transaction.getEntity("fire"));

        Entity charmeleon = transaction.putEntity("Charmeleon", pokemon);
        addResource(transaction,charmeleon,5L,pokedexNo);
        addResource(transaction,charmeleon,"When it swings its burning tail, it elevates the temperature to unbearably high levels.",description);
        addResource(transaction,charmeleon,11L,height);
        addResource(transaction,charmeleon,190L,weight);
        putTypes(transaction,charmeleon,
                transaction.getEntity("fire"));
        transaction.addRelation(evolution)
                .putRolePlayer(descendent,charmeleon)
                .putRolePlayer(ancestor, charmander);

        Entity charizard = transaction.putEntity("Charizard", pokemon);
        addResource(transaction,charizard,6L,pokedexNo);
        addResource(transaction,charizard,"Spits fire that is hot enough to melt boulders. Known to cause forest fires unintentionally.",description);
        addResource(transaction,charizard,17L,height);
        addResource(transaction,charizard,905L,weight);
        putTypes(transaction,charizard,
                transaction.getEntity("fire"),transaction.getEntity("flying"));
        transaction.addRelation(evolution)
                .putRolePlayer(descendent,charizard)
                .putRolePlayer(ancestor, charmeleon);
    }

    private static void addResource(MindmapsTransaction mt, Entity pokemon, String s, ResourceType<String> type) {
            Resource<String> resource = mt.addResource(type).setValue(s);
        mt.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, pokemon)
                .putRolePlayer(hasResourceValue, resource);
    }

    private static void addResource(MindmapsTransaction mt, Entity pokemon, Long l, ResourceType<Long> type) {
            Resource<Long> resource = mt.addResource(type).setValue(l);
        mt.addRelation(hasResource)
                .putRolePlayer(hasResourceTarget, pokemon)
                .putRolePlayer(hasResourceValue, resource);
    }

    private static void putTypes(MindmapsTransaction mt, Entity pokemon, Entity... entities) {
        for (Entity entity : entities) {
            mt.addRelation(hasType)
                    .putRolePlayer(pokemonWithType,pokemon)
                    .putRolePlayer(typeOfPokemon,entity);
        }
    }

    private static void buildRelations(MindmapsTransaction transaction) {
        Entity normal = transaction.putEntity("normal",pokemonType);
        Entity fighting = transaction.putEntity("fighting",pokemonType);
        Entity flying = transaction.putEntity("flying",pokemonType);
        Entity poison = transaction.putEntity("poison",pokemonType);
        Entity ground = transaction.putEntity("ground",pokemonType);
        Entity rock = transaction.putEntity("rock",pokemonType);
        Entity bug = transaction.putEntity("bug",pokemonType);
        Entity ghost = transaction.putEntity("ghost",pokemonType);
        Entity steel = transaction.putEntity("steel",pokemonType);
        Entity fire = transaction.putEntity("fire",pokemonType);
        Entity water = transaction.putEntity("water",pokemonType);
        Entity grass = transaction.putEntity("grass",pokemonType);
        Entity electric = transaction.putEntity("electric",pokemonType);
        Entity psychic = transaction.putEntity("psychic",pokemonType);
        Entity ice = transaction.putEntity("ice",pokemonType);
        Entity dragon = transaction.putEntity("dragon",pokemonType);
        Entity dark = transaction.putEntity("dark",pokemonType);
        Entity fairy = transaction.putEntity("fairy",pokemonType);
        Entity unknown = transaction.putEntity("unknown",pokemonType);
        Entity shadow = transaction.putEntity("shadow",pokemonType);

        putSuper(transaction,normal,fighting);
        putSuper(transaction,rock,fighting);
        putSuper(transaction,fighting,flying);
        putSuper(transaction,bug,flying);
        putSuper(transaction,grass,flying);
        putSuper(transaction,grass,poison);
        putSuper(transaction,grass,bug);
        putSuper(transaction,grass,fire);
        putSuper(transaction,fairy,poison);
        putSuper(transaction,poison,ground);
        putSuper(transaction,ice,steel);
        putSuper(transaction,poison,psychic);
        putSuper(transaction,ground,grass);
        putSuper(transaction,rock,grass);
        putSuper(transaction,water,grass);
        putSuper(transaction,grass,ice);
    }

    private static void putSuper(MindmapsTransaction mt, Entity defend, Entity attack) {
        mt.addRelation(superEffective)
                .putRolePlayer(defendingType,defend)
                .putRolePlayer(attackingType,attack);
    }
}
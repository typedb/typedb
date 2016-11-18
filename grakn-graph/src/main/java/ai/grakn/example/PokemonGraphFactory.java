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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Entity;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.util.ErrorMessage;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.exception.GraknValidationException;

/**
 * A class which loads sample data into a grakn graph
 */
public class PokemonGraphFactory{
    private static EntityType pokemon;
    private static EntityType pokemonType;
    private static ResourceType<Long> pokedexNo;
    private static ResourceType<String> description;
    private static ResourceType<String> name;
    private static ResourceType<Long> height;
    private static ResourceType<Double> weight;
    private static RoleType ancestor;
    private static RoleType descendent;
    private static RoleType pokemonWithType;
    private static RoleType typeOfPokemon;
    private static RoleType defendingType;
    private static RoleType attackingType;
    private static RelationType evolution;
    private static RelationType hasType;
    private static RelationType superEffective;
    private static Entity normal;
    private static Entity fighting;
    private static Entity flying;
    private static Entity poison;
    private static Entity ground;
    private static Entity rock;
    private static Entity bug;
    private static Entity ghost;
    private static Entity steel;
    private static Entity fire;
    private static Entity water;
    private static Entity grass;
    private static Entity electric;
    private static Entity psychic;
    private static Entity ice;
    private static Entity dragon;
    private static Entity dark;
    private static Entity fairy;
    private static Entity unknown;
    private static Entity shadow;

    private PokemonGraphFactory(){
        throw new UnsupportedOperationException();
    }

    public static void loadGraph(GraknGraph graknGraph) {
        buildGraph(graknGraph);
        try {
            graknGraph.commit();
        } catch (GraknValidationException e) {
            throw new RuntimeException(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage(), e);
        }
    }

    private static void buildGraph(GraknGraph graph) {
        buildOntology(graph);
        buildRelations(graph);
        buildInstances(graph);
    }

    private static void buildOntology(GraknGraph graph) {

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
                .playsRole(ancestor)
                .playsRole(descendent)
                .playsRole(pokemonWithType);

        pokemonType = graph.putEntityType("pokemon-type")
                .playsRole(typeOfPokemon)
                .playsRole(defendingType)
                .playsRole(attackingType);

        name = graph.putResourceType("name", ResourceType.DataType.STRING);
        pokedexNo = graph.putResourceType("pokedex-no", ResourceType.DataType.LONG);
        description = graph.putResourceType("description", ResourceType.DataType.STRING);
        height = graph.putResourceType("height", ResourceType.DataType.LONG);
        weight = graph.putResourceType("weight", ResourceType.DataType.DOUBLE);

        pokemon.hasResource(name);
        pokemon.hasResource(pokedexNo);
        pokemon.hasResource(description);
        pokemon.hasResource(height);
        pokemon.hasResource(weight);

        pokemonType.hasResource(name);
    }

    private static void buildInstances(GraknGraph graph) {
        Entity bulbasaur = pokemon.addEntity();
        addResource(graph, bulbasaur, "Bulbasaur", name);
        addResource(graph,bulbasaur,1L,pokedexNo);
        addResource(graph,bulbasaur,"A strange seed was planted on its back at birth. The plant sprouts and grows with this POKéMON.",description);
        addResource(graph,bulbasaur,7L,height);
        addResource(graph,bulbasaur,69d,weight);
        putTypes(graph,bulbasaur, poison, grass);

        Entity ivysaur = pokemon.addEntity();
        addResource(graph, ivysaur, "Ivysaur", name);
        addResource(graph,ivysaur,2L,pokedexNo);
        addResource(graph,ivysaur,"When the bulb on its back grows large, it appears to lose the ability to stand on its hind legs.",description);
        addResource(graph,ivysaur,10L,height);
        addResource(graph,ivysaur,130d,weight);
        putTypes(graph,ivysaur, poison, grass);
        evolution.addRelation()
                .putRolePlayer(descendent,ivysaur)
                .putRolePlayer(ancestor,bulbasaur);

        Entity venusaur = pokemon.addEntity();
        addResource(graph, venusaur, "Venusaur", name);
        addResource(graph,venusaur,3L,pokedexNo);
        addResource(graph,venusaur,"The plant blooms when it is absorbing solar energy. It stays on the move to seek sunlight.",description);
        addResource(graph,venusaur,20L,height);
        addResource(graph,venusaur,1000d,weight);
        putTypes(graph,venusaur, poison, grass);
        evolution.addRelation()
                .putRolePlayer(descendent,venusaur)
                .putRolePlayer(ancestor,ivysaur);

        Entity charmander = pokemon.addEntity();
        addResource(graph, charmander, "Charmander", name);
        addResource(graph,charmander,4L,pokedexNo);
        addResource(graph,charmander,"Obviously prefers hot places. When it rains, steam is said to spout from the tip of its tail.",description);
        addResource(graph,charmander,6L,height);
        addResource(graph,charmander,85d,weight);
        putTypes(graph,charmander, fire);

        Entity charmeleon = pokemon.addEntity();
        addResource(graph, charmeleon, "Charmeleon", name);
        addResource(graph,charmeleon,5L,pokedexNo);
        addResource(graph,charmeleon,"When it swings its burning tail, it elevates the temperature to unbearably high levels.",description);
        addResource(graph,charmeleon,11L,height);
        addResource(graph,charmeleon,190d,weight);
        putTypes(graph,charmeleon, fire);
        evolution.addRelation()
                .putRolePlayer(descendent,charmeleon)
                .putRolePlayer(ancestor, charmander);

        Entity charizard = pokemon.addEntity();
        addResource(graph, charizard, "Charizard", name);
        addResource(graph,charizard,6L,pokedexNo);
        addResource(graph,charizard,"Spits fire that is hot enough to melt boulders. Known to cause forest fires unintentionally.",description);
        addResource(graph,charizard,17L,height);
        addResource(graph,charizard,905d,weight);
        putTypes(graph,charizard, fire, flying);
        evolution.addRelation()
                .putRolePlayer(descendent,charizard)
                .putRolePlayer(ancestor, charmeleon);
    }

    private static <T> void addResource(GraknGraph graph, Entity entity, T s, ResourceType<T> type) {
        Resource<T> resource = type.putResource(s);
        entity.hasResource(resource);
    }

    private static void putTypes(GraknGraph graph, Entity pokemon, Entity... entities) {
        for (Entity entity : entities) {
            hasType.addRelation()
                    .putRolePlayer(pokemonWithType,pokemon)
                    .putRolePlayer(typeOfPokemon,entity);
        }
    }

    private static void buildRelations(GraknGraph graph) {
        normal = pokemonType.addEntity();
        addResource(graph, normal, "normal", name);
        fighting = pokemonType.addEntity();
        addResource(graph, fighting, "fighting", name);
        flying = pokemonType.addEntity();
        addResource(graph, flying, "flying", name);
        poison = pokemonType.addEntity();
        addResource(graph, poison, "poison", name);
        ground = pokemonType.addEntity();
        addResource(graph, ground, "ground", name);
        rock = pokemonType.addEntity();
        addResource(graph, rock, "rock", name);
        bug = pokemonType.addEntity();
        addResource(graph, bug, "bug", name);
        ghost = pokemonType.addEntity();
        addResource(graph, ghost, "ghost", name);
        steel = pokemonType.addEntity();
        addResource(graph, steel, "steel", name);
        fire = pokemonType.addEntity();
        addResource(graph, fire, "fire", name);
        water = pokemonType.addEntity();
        addResource(graph, water, "water", name);
        grass = pokemonType.addEntity();
        addResource(graph, grass, "grass", name);
        electric = pokemonType.addEntity();
        addResource(graph, electric, "electric", name);
        psychic = pokemonType.addEntity();
        addResource(graph, psychic, "psychic", name);
        ice = pokemonType.addEntity();
        addResource(graph, ice, "ice", name);
        dragon = pokemonType.addEntity();
        addResource(graph, dragon, "dragon", name);
        dark = pokemonType.addEntity();
        addResource(graph, dark, "dark", name);
        fairy = pokemonType.addEntity();
        addResource(graph, fairy, "fairy", name);
        unknown = pokemonType.addEntity();
        addResource(graph, unknown, "unknown", name);
        shadow = pokemonType.addEntity();
        addResource(graph, shadow, "shadow", name);

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

    private static void putSuper(GraknGraph graph, Entity defend, Entity attack) {
        superEffective.addRelation()
                .putRolePlayer(defendingType,defend)
                .putRolePlayer(attackingType,attack);
    }
}
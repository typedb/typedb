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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;

/**
 * <p>
 *     A sample graph.
 * </p>
 *
 * <p>
 *     A class which loads sample data based on pokemon into a grakn graph.
 * </p>
 * @author fppt
 * @author aelred
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

    public static void loadGraph(GraknGraph graph) {
        buildGraph(graph);
    }

    private static void buildGraph(GraknGraph graph) {
        buildOntology(graph);
        buildRelations();
        buildInstances();
    }

    private static void buildOntology(GraknGraph graph) {

        ancestor = graph.putRoleType("ancestor");
        descendent = graph.putRoleType("descendent");
        evolution = graph.putRelationType("evolution")
                .relates(ancestor)
                .relates(descendent);

        pokemonWithType = graph.putRoleType("pokemon-with-type");
        typeOfPokemon = graph.putRoleType("type-of-pokemon");
        hasType = graph.putRelationType("has-type")
                .relates(pokemonWithType)
                .relates(typeOfPokemon);

        defendingType = graph.putRoleType("defending-type");
        attackingType = graph.putRoleType("attacking-type");
        superEffective = graph.putRelationType("super-effective")
                .relates(defendingType)
                .relates(attackingType);

        pokemon = graph.putEntityType("pokemon")
                .plays(ancestor)
                .plays(descendent)
                .plays(pokemonWithType);

        pokemonType = graph.putEntityType("pokemon-type")
                .plays(typeOfPokemon)
                .plays(defendingType)
                .plays(attackingType);

        name = graph.putResourceType("name", ResourceType.DataType.STRING);
        pokedexNo = graph.putResourceType("pokedex-no", ResourceType.DataType.LONG);
        description = graph.putResourceType("description", ResourceType.DataType.STRING);
        height = graph.putResourceType("height", ResourceType.DataType.LONG);
        weight = graph.putResourceType("weight", ResourceType.DataType.DOUBLE);

        pokemon.resource(name);
        pokemon.resource(pokedexNo);
        pokemon.resource(description);
        pokemon.resource(height);
        pokemon.resource(weight);

        pokemonType.resource(name);
    }

    private static void buildInstances() {
        Entity bulbasaur = pokemon.addEntity();
        addResource(bulbasaur, "Bulbasaur", name);
        addResource(bulbasaur,1L,pokedexNo);
        addResource(bulbasaur,"A strange seed was planted on its back at birth. The plant sprouts and grows with this POKéMON.",description);
        addResource(bulbasaur,7L,height);
        addResource(bulbasaur,69d,weight);
        putTypes(bulbasaur, poison, grass);

        Entity ivysaur = pokemon.addEntity();
        addResource(ivysaur, "Ivysaur", name);
        addResource(ivysaur,2L,pokedexNo);
        addResource(ivysaur,"When the bulb on its back grows large, it appears to lose the ability to stand on its hind legs.",description);
        addResource(ivysaur,10L,height);
        addResource(ivysaur,130d,weight);
        putTypes(ivysaur, poison, grass);
        evolution.addRelation()
                .addRolePlayer(descendent,ivysaur)
                .addRolePlayer(ancestor,bulbasaur);

        Entity venusaur = pokemon.addEntity();
        addResource(venusaur, "Venusaur", name);
        addResource(venusaur,3L,pokedexNo);
        addResource(venusaur,"The plant blooms when it is absorbing solar energy. It stays on the move to seek sunlight.",description);
        addResource(venusaur,20L,height);
        addResource(venusaur,1000d,weight);
        putTypes(venusaur, poison, grass);
        evolution.addRelation()
                .addRolePlayer(descendent,venusaur)
                .addRolePlayer(ancestor,ivysaur);

        Entity charmander = pokemon.addEntity();
        addResource(charmander, "Charmander", name);
        addResource(charmander,4L,pokedexNo);
        addResource(charmander,"Obviously prefers hot places. When it rains, steam is said to spout from the tip of its tail.",description);
        addResource(charmander,6L,height);
        addResource(charmander,85d,weight);
        putTypes(charmander, fire);

        Entity charmeleon = pokemon.addEntity();
        addResource(charmeleon, "Charmeleon", name);
        addResource(charmeleon,5L,pokedexNo);
        addResource(charmeleon,"When it swings its burning tail, it elevates the temperature to unbearably high levels.",description);
        addResource(charmeleon,11L,height);
        addResource(charmeleon,190d,weight);
        putTypes(charmeleon, fire);
        evolution.addRelation()
                .addRolePlayer(descendent,charmeleon)
                .addRolePlayer(ancestor, charmander);

        Entity charizard = pokemon.addEntity();
        addResource(charizard, "Charizard", name);
        addResource(charizard,6L,pokedexNo);
        addResource(charizard,"Spits fire that is hot enough to melt boulders. Known to cause forest fires unintentionally.",description);
        addResource(charizard,17L,height);
        addResource(charizard,905d,weight);
        putTypes(charizard, fire, flying);
        evolution.addRelation()
                .addRolePlayer(descendent,charizard)
                .addRolePlayer(ancestor, charmeleon);
    }

    private static <T> void addResource(Entity entity, T s, ResourceType<T> type) {
        Resource<T> resource = type.putResource(s);
        entity.resource(resource);
    }

    private static void putTypes(Entity pokemon, Entity... entities) {
        for (Entity entity : entities) {
            hasType.addRelation()
                    .addRolePlayer(pokemonWithType,pokemon)
                    .addRolePlayer(typeOfPokemon,entity);
        }
    }

    private static void buildRelations() {
        normal = pokemonType.addEntity();
        addResource(normal, "normal", name);
        fighting = pokemonType.addEntity();
        addResource(fighting, "fighting", name);
        flying = pokemonType.addEntity();
        addResource(flying, "flying", name);
        poison = pokemonType.addEntity();
        addResource(poison, "poison", name);
        ground = pokemonType.addEntity();
        addResource(ground, "ground", name);
        rock = pokemonType.addEntity();
        addResource(rock, "rock", name);
        bug = pokemonType.addEntity();
        addResource(bug, "bug", name);
        ghost = pokemonType.addEntity();
        addResource(ghost, "ghost", name);
        steel = pokemonType.addEntity();
        addResource(steel, "steel", name);
        fire = pokemonType.addEntity();
        addResource(fire, "fire", name);
        water = pokemonType.addEntity();
        addResource(water, "water", name);
        grass = pokemonType.addEntity();
        addResource(grass, "grass", name);
        electric = pokemonType.addEntity();
        addResource(electric, "electric", name);
        psychic = pokemonType.addEntity();
        addResource(psychic, "psychic", name);
        ice = pokemonType.addEntity();
        addResource(ice, "ice", name);
        dragon = pokemonType.addEntity();
        addResource(dragon, "dragon", name);
        dark = pokemonType.addEntity();
        addResource(dark, "dark", name);
        fairy = pokemonType.addEntity();
        addResource(fairy, "fairy", name);
        unknown = pokemonType.addEntity();
        addResource(unknown, "unknown", name);
        shadow = pokemonType.addEntity();
        addResource(shadow, "shadow", name);

        putSuper(normal,fighting);
        putSuper(rock,fighting);
        putSuper(fighting,flying);
        putSuper(bug,flying);
        putSuper(grass,flying);
        putSuper(grass,poison);
        putSuper(grass,bug);
        putSuper(grass,fire);
        putSuper(fairy,poison);
        putSuper(poison,ground);
        putSuper(ice,steel);
        putSuper(poison,psychic);
        putSuper(ground,grass);
        putSuper(rock,grass);
        putSuper(water,grass);
        putSuper(grass,ice);
    }

    private static void putSuper(Entity defend, Entity attack) {
        superEffective.addRelation()
                .addRolePlayer(defendingType,defend)
                .addRolePlayer(attackingType,attack);
    }
}
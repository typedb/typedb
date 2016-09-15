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
import io.mindmaps.concept.Entity;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.RuleType;
import io.mindmaps.exception.MindmapsValidationException;
import io.mindmaps.util.ErrorMessage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * A class which loads sample data into a mindmaps graph
 */
public class MovieGraphFactory {
    private static MindmapsGraph mindmapsGraph;
    private static EntityType movie, person, genre, character, cluster, language;
    private static ResourceType<String> title, gender, realName, name;
    private static ResourceType<Long> tmdbVoteCount, releaseDate, runtime;
    private static ResourceType<Double> tmdbVoteAverage;
    private static RelationType hasCast, directedBy, hasGenre, hasCluster;
    private static RoleType productionBeingDirected, director, productionWithCast, actor, characterBeingPlayed;
    private static RoleType genreOfProduction, productionWithGenre, clusterOfProduction, productionWithCluster;

    private static Instance godfather, theMuppets, heat, apocalypseNow, hocusPocus, spy, chineseCoffee;
    private static Instance marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw;
    private static Instance mirandaHeart, betteMidler, sarahJessicaParker;
    private static Instance crime, drama, war, action, comedy, family, musical, fantasy;
    private static Instance donVitoCorleone, michaelCorleone, colonelWalterEKurtz, benjaminLWillard, ltVincentHanna;
    private static Instance neilMcCauley, bradleyFine, nancyBArtingstall, winifred, sarah, harry;
    private static Instance cluster0, cluster1;

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

    private MovieGraphFactory(){
        throw new UnsupportedOperationException();
    }

    public static void loadGraph(MindmapsGraph mindmapsGraph) {
        MovieGraphFactory.mindmapsGraph = mindmapsGraph;
        buildGraph();
        try {
            MovieGraphFactory.mindmapsGraph.commit();
        } catch (MindmapsValidationException e) {
            throw new RuntimeException(ErrorMessage.CANNOT_LOAD_EXAMPLE.getMessage(), e);
        }
    }

    private static void buildGraph() {
        buildOntology();
        try {
            buildInstances();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        buildRelations();
        buildRules();
    }

    private static void buildOntology() {

        productionBeingDirected = mindmapsGraph.putRoleType("production-being-directed");
        director = mindmapsGraph.putRoleType("director");
        directedBy = mindmapsGraph.putRelationType("directed-by")
                .hasRole(productionBeingDirected).hasRole(director);

        productionWithCast = mindmapsGraph.putRoleType("production-with-cast");
        actor = mindmapsGraph.putRoleType("actor");
        characterBeingPlayed = mindmapsGraph.putRoleType("character-being-played");
        hasCast = mindmapsGraph.putRelationType("has-cast")
                .hasRole(productionWithCast).hasRole(actor).hasRole(characterBeingPlayed);

        genreOfProduction = mindmapsGraph.putRoleType("genre-of-production");
        productionWithGenre = mindmapsGraph.putRoleType("production-with-genre");
        hasGenre = mindmapsGraph.putRelationType("has-genre")
                .hasRole(genreOfProduction).hasRole(productionWithGenre);

        clusterOfProduction = mindmapsGraph.putRoleType("cluster-of-production");
        productionWithCluster = mindmapsGraph.putRoleType("production-with-cluster");
        hasCluster = mindmapsGraph.putRelationType("has-cluster")
                .hasRole(clusterOfProduction).hasRole(productionWithCluster);

        title = mindmapsGraph.putResourceType("title", ResourceType.DataType.STRING);
        tmdbVoteCount = mindmapsGraph.putResourceType("tmdb-vote-count", ResourceType.DataType.LONG);
        tmdbVoteAverage = mindmapsGraph.putResourceType("tmdb-vote-average", ResourceType.DataType.DOUBLE);
        releaseDate = mindmapsGraph.putResourceType("release-date", ResourceType.DataType.LONG);
        runtime = mindmapsGraph.putResourceType("runtime", ResourceType.DataType.LONG);
        gender = mindmapsGraph.putResourceType("gender", ResourceType.DataType.STRING).setRegex("(fe)?male");
        realName = mindmapsGraph.putResourceType("real-name", ResourceType.DataType.STRING);
        name = mindmapsGraph.putResourceType("name", ResourceType.DataType.STRING);

        EntityType production = mindmapsGraph.putEntityType("production")
                .playsRole(productionWithCluster).playsRole(productionBeingDirected).playsRole(productionWithCast)
                .playsRole(productionWithGenre);

        hasResource(production, title);
        hasResource(production, tmdbVoteCount);
        hasResource(production, tmdbVoteAverage);
        hasResource(production, releaseDate);
        hasResource(production, runtime);

        movie = mindmapsGraph.putEntityType("movie").superType(production);

        mindmapsGraph.putEntityType("tv-show").superType(production);

        person = mindmapsGraph.putEntityType("person")
                .playsRole(director).playsRole(actor).playsRole(characterBeingPlayed);

        hasResource(person, gender);
        hasResource(person, name);
        hasResource(person, realName);

        genre = mindmapsGraph.putEntityType("genre").playsRole(genreOfProduction);

        hasResource(genre, name);

        character = mindmapsGraph.putEntityType("character")
                .playsRole(characterBeingPlayed);

        hasResource(character, name);

        mindmapsGraph.putEntityType("award");
        language = mindmapsGraph.putEntityType("language");

        hasResource(language, name);

        cluster = mindmapsGraph.putEntityType("cluster").playsRole(clusterOfProduction);
    }

    private static void buildInstances() throws ParseException {
        godfather = putEntityWithName(movie, "Godfather");
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, DATE_FORMAT.parse("Sun Jan 01 00:00:00 GMT 1984").getTime());

        theMuppets = putEntityWithName(movie, "The-Muppets");
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, DATE_FORMAT.parse("Sat Feb 02 00:00:00 GMT 1985").getTime());

        apocalypseNow = putEntityWithName(movie, "Apocalypse-Now");
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = putEntityWithName(movie, "Heat");
        putResource(heat, title, "Heat");

        hocusPocus = putEntityWithName(movie, "Hocus-Pocus");
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = putEntityWithName(movie, "Spy");
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime());

        chineseCoffee = putEntityWithName(movie, "Chinese-Coffee");
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, DATE_FORMAT.parse("Sat Sep 02 00:00:00 GMT 2000").getTime());

        marlonBrando = putEntityWithName(person, "Marlon-Brando");
        alPacino = putEntityWithName(person, "Al-Pacino");
        missPiggy = putEntityWithName(person, "Miss-Piggy");
        kermitTheFrog = putEntityWithName(person, "Kermit-The-Frog");
        martinSheen = putEntityWithName(person, "Martin-Sheen");
        robertDeNiro = putEntityWithName(person, "Robert-de-Niro");
        judeLaw = putEntityWithName(person, "Jude-Law");
        mirandaHeart = putEntityWithName(person, "Miranda-Heart");
        betteMidler = putEntityWithName(person, "Bette-Midler");
        sarahJessicaParker = putEntityWithName(person, "Sarah-Jessica-Parker");

        crime = putEntityWithName(genre, "crime");
        drama = putEntityWithName(genre, "drama");
        war = putEntityWithName(genre, "war");
        action = putEntityWithName(genre, "action");
        comedy = putEntityWithName(genre, "comedy");
        family = putEntityWithName(genre, "family");
        musical = putEntityWithName(genre, "musical");
        fantasy = putEntityWithName(genre, "fantasy");

        donVitoCorleone = putEntityWithName(character, "Don Vito Corleone");
        michaelCorleone = putEntityWithName(character, "Michael Corleone");
        colonelWalterEKurtz = putEntityWithName(character, "Colonel Walter E. Kurtz");
        benjaminLWillard = putEntityWithName(character, "Benjamin L. Willard");
        ltVincentHanna = putEntityWithName(character, "Lt Vincent Hanna");
        neilMcCauley = putEntityWithName(character, "Neil McCauley");
        bradleyFine = putEntityWithName(character, "Bradley Fine");
        nancyBArtingstall = putEntityWithName(character, "Nancy B Artingstall");
        winifred = putEntityWithName(character, "Winifred");
        sarah = putEntityWithName(character, "Sarah");
        harry = putEntityWithName(character, "Harry");

        cluster0 = putEntityWithName(cluster, "0");
        cluster1 = putEntityWithName(cluster, "1");
    }

    private static void buildRelations() {
        mindmapsGraph.addRelation(directedBy)
                .putRolePlayer(productionBeingDirected, chineseCoffee)
                .putRolePlayer(director, alPacino);

        hasCast(godfather, marlonBrando, donVitoCorleone);
        hasCast(godfather, alPacino, michaelCorleone);
        hasCast(theMuppets, missPiggy, missPiggy);
        hasCast(theMuppets, kermitTheFrog, kermitTheFrog);
        hasCast(apocalypseNow, marlonBrando, colonelWalterEKurtz);
        hasCast(apocalypseNow, martinSheen, benjaminLWillard);
        hasCast(heat, alPacino, ltVincentHanna);
        hasCast(heat, robertDeNiro, neilMcCauley);
        hasCast(spy, judeLaw, bradleyFine);
        hasCast(spy, mirandaHeart, nancyBArtingstall);
        hasCast(hocusPocus, betteMidler, winifred);
        hasCast(hocusPocus, sarahJessicaParker, sarah);
        hasCast(chineseCoffee, alPacino, harry);

        hasGenre(godfather, crime);
        hasGenre(godfather, drama);
        hasGenre(apocalypseNow, drama);
        hasGenre(apocalypseNow, war);
        hasGenre(heat, crime);
        hasGenre(heat, drama);
        hasGenre(heat, action);
        hasGenre(theMuppets, comedy);
        hasGenre(theMuppets, family);
        hasGenre(theMuppets, musical);
        hasGenre(hocusPocus, comedy);
        hasGenre(hocusPocus, family);
        hasGenre(hocusPocus, fantasy);
        hasGenre(spy, comedy);
        hasGenre(spy, family);
        hasGenre(spy, musical);
        hasGenre(chineseCoffee, drama);

        hasCluster(godfather, cluster0);
        hasCluster(apocalypseNow, cluster0);
        hasCluster(heat, cluster0);
        hasCluster(theMuppets, cluster1);
        hasCluster(hocusPocus, cluster1);
    }

    private static void buildRules() {
        // These rules are totally made up for testing purposes and don't work!
        RuleType aRuleType = mindmapsGraph.putRuleType("a-rule-type");

        mindmapsGraph.putRule("expectation-rule", "expect-lhs", "expect-rhs", aRuleType)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        mindmapsGraph.putRule("materialize-rule", "materialize-lhs", "materialize-rhs", aRuleType)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);
    }

    private static Entity putEntityWithName(EntityType type, String value) {
        Entity entity = mindmapsGraph.addEntity(type);
        putResource(entity, name, value);
        return entity;
    }

    private static void hasResource(EntityType type, ResourceType<?> resourceType) {
        RoleType owner = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-value");
        mindmapsGraph.putRelationType("has-" + resourceType.getId()).hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = mindmapsGraph.putResource(resource, resourceType);

        RoleType owner = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-value");

        instance.type().playsRole(owner);

        RelationType relationType = mindmapsGraph.putRelationType("has-" + resourceType.getId())
                .hasRole(owner).hasRole(value);

        mindmapsGraph.addRelation(relationType)
                .putRolePlayer(owner, instance)
                .putRolePlayer(value, resourceInstance);
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        mindmapsGraph.addRelation(hasCast)
                .putRolePlayer(productionWithCast, movie)
                .putRolePlayer(actor, person)
                .putRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        mindmapsGraph.addRelation(hasGenre)
                .putRolePlayer(productionWithGenre, movie)
                .putRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance movie, Instance cluster) {
        mindmapsGraph.addRelation(hasCluster)
                .putRolePlayer(productionWithCluster, movie)
                .putRolePlayer(clusterOfProduction, cluster);
    }
}

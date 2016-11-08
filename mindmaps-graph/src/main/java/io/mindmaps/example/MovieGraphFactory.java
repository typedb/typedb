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
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.Instance;
import io.mindmaps.concept.RelationType;
import io.mindmaps.concept.Resource;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Rule;
import io.mindmaps.concept.RuleType;
import io.mindmaps.concept.Type;
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
        hasResource(cluster, name);
    }

    private static void buildInstances() throws ParseException {
        godfather = mindmapsGraph.addEntity(movie);
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, DATE_FORMAT.parse("Sun Jan 01 00:00:00 GMT 1984").getTime());

        theMuppets = mindmapsGraph.addEntity(movie);
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, DATE_FORMAT.parse("Sat Feb 02 00:00:00 GMT 1985").getTime());

        apocalypseNow = mindmapsGraph.addEntity(movie);
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = mindmapsGraph.addEntity(movie);
        putResource(heat, title, "Heat");

        hocusPocus = mindmapsGraph.addEntity(movie);
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = mindmapsGraph.addEntity(movie);
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime());

        chineseCoffee = mindmapsGraph.addEntity(movie);
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, DATE_FORMAT.parse("Sat Sep 02 00:00:00 GMT 2000").getTime());

        marlonBrando = mindmapsGraph.addEntity(person);
        putResource(marlonBrando, name, "Marlon Brando");
        alPacino = mindmapsGraph.addEntity(person);
        putResource(alPacino, name, "Al Pacino");
        missPiggy = mindmapsGraph.addEntity(person);
        putResource(missPiggy, name, "Miss Piggy");
        kermitTheFrog = mindmapsGraph.addEntity(person);
        putResource(kermitTheFrog, name, "Kermit The Frog");
        martinSheen = mindmapsGraph.addEntity(person);
        putResource(martinSheen, name, "Martin Sheen");
        robertDeNiro = mindmapsGraph.addEntity(person);
        putResource(robertDeNiro, name, "Robert de Niro");
        judeLaw = mindmapsGraph.addEntity(person);
        putResource(judeLaw, name, "Jude Law");
        mirandaHeart = mindmapsGraph.addEntity(person);
        putResource(mirandaHeart, name, "Miranda Heart");
        betteMidler = mindmapsGraph.addEntity(person);
        putResource(betteMidler, name, "Bette Midler");
        sarahJessicaParker = mindmapsGraph.addEntity(person);
        putResource(sarahJessicaParker, name, "Sarah Jessica Parker");

        crime = mindmapsGraph.addEntity(genre);
        putResource(crime, name, "crime");
        drama = mindmapsGraph.addEntity(genre);
        putResource(drama, name, "drama");
        war = mindmapsGraph.addEntity(genre);
        putResource(war, name, "war");
        action = mindmapsGraph.addEntity(genre);
        putResource(action, name, "action");
        comedy = mindmapsGraph.addEntity(genre);
        putResource(comedy, name, "comedy");
        family = mindmapsGraph.addEntity(genre);
        putResource(family, name, "family");
        musical = mindmapsGraph.addEntity(genre);
        putResource(musical, name, "musical");
        fantasy = mindmapsGraph.addEntity(genre);
        putResource(fantasy, name, "fantasy");

        donVitoCorleone = mindmapsGraph.addEntity(character);
        putResource(donVitoCorleone, name, "Don Vito Corleone");
        michaelCorleone = mindmapsGraph.addEntity(character);
        putResource(michaelCorleone, name, "Michael Corleone");
        colonelWalterEKurtz = mindmapsGraph.addEntity(character);
        putResource(colonelWalterEKurtz, name, "Colonel Walter E. Kurtz");
        benjaminLWillard = mindmapsGraph.addEntity(character);
        putResource(benjaminLWillard, name, "Benjamin L. Willard");
        ltVincentHanna = mindmapsGraph.addEntity(character);
        putResource(ltVincentHanna, name, "Lt Vincent Hanna");
        neilMcCauley = mindmapsGraph.addEntity(character);
        putResource(neilMcCauley, name, "Neil McCauley");
        bradleyFine = mindmapsGraph.addEntity(character);
        putResource(bradleyFine, name, "Bradley Fine");
        nancyBArtingstall = mindmapsGraph.addEntity(character);
        putResource(nancyBArtingstall, name, "Nancy B Artingstall");
        winifred = mindmapsGraph.addEntity(character);
        putResource(winifred, name, "Winifred");
        sarah = mindmapsGraph.addEntity(character);
        putResource(sarah, name, "Sarah");
        harry = mindmapsGraph.addEntity(character);
        putResource(harry, name, "Harry");

        cluster0 = mindmapsGraph.addEntity(cluster);
        cluster1 = mindmapsGraph.addEntity(cluster);
        putResource(cluster0, name, "0");
        putResource(cluster1, name, "1");
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
        hasResource(aRuleType, name);

        Rule expectation = mindmapsGraph.addRule("$x id 'expect-lhs';", "$x id 'expect-rhs';", aRuleType)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        putResource(expectation, name, "expectation-rule");

        Rule materialize = mindmapsGraph.addRule("$x id 'materialize-lhs';", "$x id 'materialize-rhs';", aRuleType)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);

        putResource(materialize, name, "materialize-rule");
    }

    private static void hasResource(Type type, ResourceType<?> resourceType) {
        RoleType owner = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = mindmapsGraph.putRoleType("has-" + resourceType.getId() + "-value");
        mindmapsGraph.putRelationType("has-" + resourceType.getId()).hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = mindmapsGraph.putResource(resource, resourceType);
        instance.hasResource(resourceInstance);
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

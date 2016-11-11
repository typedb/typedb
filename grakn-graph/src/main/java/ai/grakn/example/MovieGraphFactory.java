/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs
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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * A class which loads sample data into a mindmaps graph
 */
public class MovieGraphFactory {
    private static GraknGraph graknGraph;
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

    public static void loadGraph(GraknGraph graknGraph) {
        MovieGraphFactory.graknGraph = graknGraph;
        buildGraph();
        try {
            MovieGraphFactory.graknGraph.commit();
        } catch (GraknValidationException e) {
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

        productionBeingDirected = graknGraph.putRoleType("production-being-directed");
        director = graknGraph.putRoleType("director");
        directedBy = graknGraph.putRelationType("directed-by")
                .hasRole(productionBeingDirected).hasRole(director);

        productionWithCast = graknGraph.putRoleType("production-with-cast");
        actor = graknGraph.putRoleType("actor");
        characterBeingPlayed = graknGraph.putRoleType("character-being-played");
        hasCast = graknGraph.putRelationType("has-cast")
                .hasRole(productionWithCast).hasRole(actor).hasRole(characterBeingPlayed);

        genreOfProduction = graknGraph.putRoleType("genre-of-production");
        productionWithGenre = graknGraph.putRoleType("production-with-genre");
        hasGenre = graknGraph.putRelationType("has-genre")
                .hasRole(genreOfProduction).hasRole(productionWithGenre);

        clusterOfProduction = graknGraph.putRoleType("cluster-of-production");
        productionWithCluster = graknGraph.putRoleType("production-with-cluster");
        hasCluster = graknGraph.putRelationType("has-cluster")
                .hasRole(clusterOfProduction).hasRole(productionWithCluster);

        title = graknGraph.putResourceType("title", ResourceType.DataType.STRING);
        tmdbVoteCount = graknGraph.putResourceType("tmdb-vote-count", ResourceType.DataType.LONG);
        tmdbVoteAverage = graknGraph.putResourceType("tmdb-vote-average", ResourceType.DataType.DOUBLE);
        releaseDate = graknGraph.putResourceType("release-date", ResourceType.DataType.LONG);
        runtime = graknGraph.putResourceType("runtime", ResourceType.DataType.LONG);
        gender = graknGraph.putResourceType("gender", ResourceType.DataType.STRING).setRegex("(fe)?male");
        realName = graknGraph.putResourceType("real-name", ResourceType.DataType.STRING);
        name = graknGraph.putResourceType("name", ResourceType.DataType.STRING);

        EntityType production = graknGraph.putEntityType("production")
                .playsRole(productionWithCluster).playsRole(productionBeingDirected).playsRole(productionWithCast)
                .playsRole(productionWithGenre);

        hasResource(production, title);
        hasResource(production, tmdbVoteCount);
        hasResource(production, tmdbVoteAverage);
        hasResource(production, releaseDate);
        hasResource(production, runtime);

        movie = graknGraph.putEntityType("movie").superType(production);

        graknGraph.putEntityType("tv-show").superType(production);

        person = graknGraph.putEntityType("person")
                .playsRole(director).playsRole(actor).playsRole(characterBeingPlayed);

        hasResource(person, gender);
        hasResource(person, name);
        hasResource(person, realName);

        genre = graknGraph.putEntityType("genre").playsRole(genreOfProduction);

        hasResource(genre, name);

        character = graknGraph.putEntityType("character")
                .playsRole(characterBeingPlayed);

        hasResource(character, name);

        graknGraph.putEntityType("award");
        language = graknGraph.putEntityType("language");

        hasResource(language, name);

        cluster = graknGraph.putEntityType("cluster").playsRole(clusterOfProduction);
        hasResource(cluster, name);
    }

    private static void buildInstances() throws ParseException {
        godfather = graknGraph.addEntity(movie);
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, DATE_FORMAT.parse("Sun Jan 01 00:00:00 GMT 1984").getTime());

        theMuppets = graknGraph.addEntity(movie);
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, DATE_FORMAT.parse("Sat Feb 02 00:00:00 GMT 1985").getTime());

        apocalypseNow = graknGraph.addEntity(movie);
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = graknGraph.addEntity(movie);
        putResource(heat, title, "Heat");

        hocusPocus = graknGraph.addEntity(movie);
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = graknGraph.addEntity(movie);
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime());

        chineseCoffee = graknGraph.addEntity(movie);
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, DATE_FORMAT.parse("Sat Sep 02 00:00:00 GMT 2000").getTime());

        marlonBrando = graknGraph.addEntity(person);
        putResource(marlonBrando, name, "Marlon Brando");
        alPacino = graknGraph.addEntity(person);
        putResource(alPacino, name, "Al Pacino");
        missPiggy = graknGraph.addEntity(person);
        putResource(missPiggy, name, "Miss Piggy");
        kermitTheFrog = graknGraph.addEntity(person);
        putResource(kermitTheFrog, name, "Kermit The Frog");
        martinSheen = graknGraph.addEntity(person);
        putResource(martinSheen, name, "Martin Sheen");
        robertDeNiro = graknGraph.addEntity(person);
        putResource(robertDeNiro, name, "Robert de Niro");
        judeLaw = graknGraph.addEntity(person);
        putResource(judeLaw, name, "Jude Law");
        mirandaHeart = graknGraph.addEntity(person);
        putResource(mirandaHeart, name, "Miranda Heart");
        betteMidler = graknGraph.addEntity(person);
        putResource(betteMidler, name, "Bette Midler");
        sarahJessicaParker = graknGraph.addEntity(person);
        putResource(sarahJessicaParker, name, "Sarah Jessica Parker");

        crime = graknGraph.addEntity(genre);
        putResource(crime, name, "crime");
        drama = graknGraph.addEntity(genre);
        putResource(drama, name, "drama");
        war = graknGraph.addEntity(genre);
        putResource(war, name, "war");
        action = graknGraph.addEntity(genre);
        putResource(action, name, "action");
        comedy = graknGraph.addEntity(genre);
        putResource(comedy, name, "comedy");
        family = graknGraph.addEntity(genre);
        putResource(family, name, "family");
        musical = graknGraph.addEntity(genre);
        putResource(musical, name, "musical");
        fantasy = graknGraph.addEntity(genre);
        putResource(fantasy, name, "fantasy");

        donVitoCorleone = graknGraph.addEntity(character);
        putResource(donVitoCorleone, name, "Don Vito Corleone");
        michaelCorleone = graknGraph.addEntity(character);
        putResource(michaelCorleone, name, "Michael Corleone");
        colonelWalterEKurtz = graknGraph.addEntity(character);
        putResource(colonelWalterEKurtz, name, "Colonel Walter E. Kurtz");
        benjaminLWillard = graknGraph.addEntity(character);
        putResource(benjaminLWillard, name, "Benjamin L. Willard");
        ltVincentHanna = graknGraph.addEntity(character);
        putResource(ltVincentHanna, name, "Lt Vincent Hanna");
        neilMcCauley = graknGraph.addEntity(character);
        putResource(neilMcCauley, name, "Neil McCauley");
        bradleyFine = graknGraph.addEntity(character);
        putResource(bradleyFine, name, "Bradley Fine");
        nancyBArtingstall = graknGraph.addEntity(character);
        putResource(nancyBArtingstall, name, "Nancy B Artingstall");
        winifred = graknGraph.addEntity(character);
        putResource(winifred, name, "Winifred");
        sarah = graknGraph.addEntity(character);
        putResource(sarah, name, "Sarah");
        harry = graknGraph.addEntity(character);
        putResource(harry, name, "Harry");

        cluster0 = graknGraph.addEntity(cluster);
        cluster1 = graknGraph.addEntity(cluster);
        putResource(cluster0, name, "0");
        putResource(cluster1, name, "1");
    }

    private static void buildRelations() {
        graknGraph.addRelation(directedBy)
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
        RuleType aRuleType = graknGraph.putRuleType("a-rule-type");
        hasResource(aRuleType, name);

        Pattern lhs = graknGraph.graql().parsePattern("$x id 'expect-lhs'");
        Pattern rhs = graknGraph.graql().parsePattern("$x id 'expect-rhs'");

        Rule expectation = graknGraph.addRule(lhs, rhs, aRuleType)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        putResource(expectation, name, "expectation-rule");

        lhs = graknGraph.graql().parsePattern("$x id 'materialize-lhs'");
        rhs = graknGraph.graql().parsePattern("$x id 'materialize-rhs'");
        Rule materialize = graknGraph.addRule(lhs, rhs, aRuleType)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);

        putResource(materialize, name, "materialize-rule");
    }

    private static void hasResource(Type type, ResourceType<?> resourceType) {
        RoleType owner = graknGraph.putRoleType("has-" + resourceType.getId() + "-owner");
        RoleType value = graknGraph.putRoleType("has-" + resourceType.getId() + "-value");
        graknGraph.putRelationType("has-" + resourceType.getId()).hasRole(owner).hasRole(value);

        type.playsRole(owner);
        resourceType.playsRole(value);
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = graknGraph.putResource(resource, resourceType);
        instance.hasResource(resourceInstance);
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        graknGraph.addRelation(hasCast)
                .putRolePlayer(productionWithCast, movie)
                .putRolePlayer(actor, person)
                .putRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        graknGraph.addRelation(hasGenre)
                .putRolePlayer(productionWithGenre, movie)
                .putRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance movie, Instance cluster) {
        graknGraph.addRelation(hasCluster)
                .putRolePlayer(productionWithCluster, movie)
                .putRolePlayer(clusterOfProduction, cluster);
    }
}

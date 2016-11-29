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
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.exception.GraknValidationException;
import ai.grakn.graql.Pattern;
import ai.grakn.util.ErrorMessage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * A class which loads sample data into a grakn graph
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

        production.hasResource(title);
        production.hasResource(tmdbVoteCount);
        production.hasResource(tmdbVoteAverage);
        production.hasResource(releaseDate);
        production.hasResource(runtime);

        movie = graknGraph.putEntityType("movie").superType(production);

        graknGraph.putEntityType("tv-show").superType(production);

        person = graknGraph.putEntityType("person")
                .playsRole(director).playsRole(actor).playsRole(characterBeingPlayed);

        person.hasResource(gender);
        person.hasResource(name);
        person.hasResource(realName);

        genre = graknGraph.putEntityType("genre").playsRole(genreOfProduction);

        genre.key(name);

        character = graknGraph.putEntityType("character")
                .playsRole(characterBeingPlayed);

        character.hasResource(name);

        graknGraph.putEntityType("award");
        language = graknGraph.putEntityType("language");

        language.hasResource(name);

        cluster = graknGraph.putEntityType("cluster").playsRole(clusterOfProduction);
        cluster.hasResource(name);
    }

    private static void buildInstances() throws ParseException {
        godfather = movie.addEntity();
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, DATE_FORMAT.parse("Sun Jan 01 00:00:00 GMT 1984").getTime());

        theMuppets = movie.addEntity();
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, DATE_FORMAT.parse("Sat Feb 02 00:00:00 GMT 1985").getTime());

        apocalypseNow = movie.addEntity();
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = movie.addEntity();
        putResource(heat, title, "Heat");

        hocusPocus = movie.addEntity();
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = movie.addEntity();
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, DATE_FORMAT.parse("Mon Mar 03 00:00:00 BST 1986").getTime());

        chineseCoffee = movie.addEntity();
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, DATE_FORMAT.parse("Sat Sep 02 00:00:00 GMT 2000").getTime());

        marlonBrando = person.addEntity();
        putResource(marlonBrando, name, "Marlon Brando");
        alPacino = person.addEntity();
        putResource(alPacino, name, "Al Pacino");
        missPiggy = person.addEntity();
        putResource(missPiggy, name, "Miss Piggy");
        kermitTheFrog = person.addEntity();
        putResource(kermitTheFrog, name, "Kermit The Frog");
        martinSheen = person.addEntity();
        putResource(martinSheen, name, "Martin Sheen");
        robertDeNiro = person.addEntity();
        putResource(robertDeNiro, name, "Robert de Niro");
        judeLaw = person.addEntity();
        putResource(judeLaw, name, "Jude Law");
        mirandaHeart = person.addEntity();
        putResource(mirandaHeart, name, "Miranda Heart");
        betteMidler = person.addEntity();
        putResource(betteMidler, name, "Bette Midler");
        sarahJessicaParker = person.addEntity();
        putResource(sarahJessicaParker, name, "Sarah Jessica Parker");

        crime = genre.addEntity();
        putResource(crime, name, "crime");
        drama = genre.addEntity();
        putResource(drama, name, "drama");
        war = genre.addEntity();
        putResource(war, name, "war");
        action = genre.addEntity();
        putResource(action, name, "action");
        comedy = genre.addEntity();
        putResource(comedy, name, "comedy");
        family = genre.addEntity();
        putResource(family, name, "family");
        musical = genre.addEntity();
        putResource(musical, name, "musical");
        fantasy = genre.addEntity();
        putResource(fantasy, name, "fantasy");

        donVitoCorleone = character.addEntity();
        putResource(donVitoCorleone, name, "Don Vito Corleone");
        michaelCorleone = character.addEntity();
        putResource(michaelCorleone, name, "Michael Corleone");
        colonelWalterEKurtz = character.addEntity();
        putResource(colonelWalterEKurtz, name, "Colonel Walter E. Kurtz");
        benjaminLWillard = character.addEntity();
        putResource(benjaminLWillard, name, "Benjamin L. Willard");
        ltVincentHanna = character.addEntity();
        putResource(ltVincentHanna, name, "Lt Vincent Hanna");
        neilMcCauley = character.addEntity();
        putResource(neilMcCauley, name, "Neil McCauley");
        bradleyFine = character.addEntity();
        putResource(bradleyFine, name, "Bradley Fine");
        nancyBArtingstall = character.addEntity();
        putResource(nancyBArtingstall, name, "Nancy B Artingstall");
        winifred = character.addEntity();
        putResource(winifred, name, "Winifred");
        sarah = character.addEntity();
        putResource(sarah, name, "Sarah");
        harry = character.addEntity();
        putResource(harry, name, "Harry");

        cluster0 = cluster.addEntity();
        cluster1 = cluster.addEntity();
        putResource(cluster0, name, "0");
        putResource(cluster1, name, "1");
    }

    private static void buildRelations() {
        directedBy.addRelation()
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
        aRuleType.hasResource(name);

        Pattern lhs = graknGraph.graql().parsePattern("$x id 'expect-lhs'");
        Pattern rhs = graknGraph.graql().parsePattern("$x id 'expect-rhs'");

        Rule expectation = aRuleType.addRule(lhs, rhs)
                .setExpectation(true)
                .addConclusion(movie).addHypothesis(person);

        putResource(expectation, name, "expectation-rule");

        lhs = graknGraph.graql().parsePattern("$x id 'materialize-lhs'");
        rhs = graknGraph.graql().parsePattern("$x id 'materialize-rhs'");
        Rule materialize = aRuleType.addRule(lhs, rhs)
                .setMaterialise(true)
                .addConclusion(person).addConclusion(genre).addHypothesis(hasCast);

        putResource(materialize, name, "materialize-rule");
    }

    private static <D> void putResource(Instance instance, ResourceType<D> resourceType, D resource) {
        Resource resourceInstance = resourceType.putResource(resource);
        instance.hasResource(resourceInstance);
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        hasCast.addRelation()
                .putRolePlayer(productionWithCast, movie)
                .putRolePlayer(actor, person)
                .putRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        hasGenre.addRelation()
                .putRolePlayer(productionWithGenre, movie)
                .putRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance movie, Instance cluster) {
        hasCluster.addRelation()
                .putRolePlayer(productionWithCluster, movie)
                .putRolePlayer(clusterOfProduction, cluster);
    }
}

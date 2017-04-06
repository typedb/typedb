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

package ai.grakn.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.function.Consumer;

public class MovieGraph extends TestGraph {

    private static EntityType production, movie, person, genre, character, cluster, language;
    private static ResourceType<String> title, gender, realName, name;
    private static ResourceType<Long> tmdbVoteCount, releaseDate, runtime;
    private static ResourceType<Double> tmdbVoteAverage;
    private static RelationType hasCast, directedBy, hasGenre, hasCluster;
    private static RoleType productionBeingDirected, director, productionWithCast, actor, characterBeingPlayed;
    private static RoleType genreOfProduction, productionWithGenre, clusterOfProduction, productionWithCluster;
    private static RuleType aRuleType;

    private static Instance godfather, theMuppets, heat, apocalypseNow, hocusPocus, spy, chineseCoffee;
    private static Instance marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw;
    private static Instance mirandaHeart, betteMidler, sarahJessicaParker;
    private static Instance crime, drama, war, action, comedy, family, musical, fantasy;
    private static Instance donVitoCorleone, michaelCorleone, colonelWalterEKurtz, benjaminLWillard, ltVincentHanna;
    private static Instance neilMcCauley, bradleyFine, nancyBArtingstall, winifred, sarah, harry;
    private static Instance cluster0, cluster1;

    public static Consumer<GraknGraph> get(){
        return new MovieGraph().build();
    }

    private static long date(String dateString) {
        try {
            return new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US).parse(dateString).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void buildOntology(GraknGraph graph) {
        productionBeingDirected = graph.putRoleType("production-being-directed");
        director = graph.putRoleType("director");
        directedBy = graph.putRelationType("directed-by")
                .relates(productionBeingDirected).relates(director);

        productionWithCast = graph.putRoleType("production-with-cast");
        actor = graph.putRoleType("actor");
        characterBeingPlayed = graph.putRoleType("character-being-played");
        hasCast = graph.putRelationType("has-cast")
                .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);

        genreOfProduction = graph.putRoleType("genre-of-production");
        productionWithGenre = graph.putRoleType("production-with-genre");
        hasGenre = graph.putRelationType("has-genre")
                .relates(genreOfProduction).relates(productionWithGenre);

        clusterOfProduction = graph.putRoleType("cluster-of-production");
        productionWithCluster = graph.putRoleType("production-with-cluster");
        hasCluster = graph.putRelationType("has-cluster")
                .relates(clusterOfProduction).relates(productionWithCluster);

        title = graph.putResourceType("title", ResourceType.DataType.STRING);
        tmdbVoteCount = graph.putResourceType("tmdb-vote-count", ResourceType.DataType.LONG);
        tmdbVoteAverage = graph.putResourceType("tmdb-vote-average", ResourceType.DataType.DOUBLE);
        releaseDate = graph.putResourceType("release-date", ResourceType.DataType.LONG);
        runtime = graph.putResourceType("runtime", ResourceType.DataType.LONG);
        gender = graph.putResourceType("gender", ResourceType.DataType.STRING).setRegex("(fe)?male");
        realName = graph.putResourceType("real-name", ResourceType.DataType.STRING);
        name = graph.putResourceType("name", ResourceType.DataType.STRING);

        production = graph.putEntityType("production")
                .plays(productionWithCluster).plays(productionBeingDirected).plays(productionWithCast)
                .plays(productionWithGenre);

        production.resource(title);
        production.resource(tmdbVoteCount);
        production.resource(tmdbVoteAverage);
        production.resource(releaseDate);
        production.resource(runtime);

        movie = graph.putEntityType("movie").superType(production);

        graph.putEntityType("tv-show").superType(production);

        person = graph.putEntityType("person")
                .plays(director).plays(actor).plays(characterBeingPlayed);

        person.resource(gender);
        person.resource(name);
        person.resource(realName);

        genre = graph.putEntityType("genre").plays(genreOfProduction);
        genre.key(name);

        character = graph.putEntityType("character")
                .plays(characterBeingPlayed);

        character.resource(name);

        graph.putEntityType("award");
        language = graph.putEntityType("language");

        language.resource(name);

        cluster = graph.putEntityType("cluster").plays(clusterOfProduction);
        cluster.resource(name);
    }

    @Override
    protected void buildInstances(GraknGraph graph) {
        godfather = movie.addEntity();
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, date("Sun Jan 01 00:00:00 GMT 1984"));

        theMuppets = movie.addEntity();
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, date("Sat Feb 02 00:00:00 GMT 1985"));

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
        putResource(spy, releaseDate, date("Mon Mar 03 00:00:00 BST 1986"));

        chineseCoffee = movie.addEntity();
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, date("Sat Sep 02 00:00:00 GMT 2000"));

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

    @Override
    protected void buildRelations(GraknGraph graph) {
        directedBy.addRelation()
                .addRolePlayer(productionBeingDirected, chineseCoffee)
                .addRolePlayer(director, alPacino);

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

        hasCluster(cluster0, godfather, apocalypseNow, heat);
        hasCluster(cluster1, theMuppets, hocusPocus);
    }

    @Override
    protected void buildRules(GraknGraph graph) {
        // These rules are totally made up for testing purposes and don't work!
        aRuleType = graph.putRuleType("a-rule-type");
        aRuleType.resource(name);

        Pattern lhs = graph.graql().parsePattern("$x id 'expect-lhs'");
        Pattern rhs = graph.graql().parsePattern("$x id 'expect-rhs'");

        Rule expectation = aRuleType.putRule(lhs, rhs);

        putResource(expectation, name, "expectation-rule");

        lhs = graph.graql().parsePattern("$x id 'materialize-lhs'");
        rhs = graph.graql().parsePattern("$x id 'materialize-rhs'");
        Rule materialize = aRuleType.putRule(lhs, rhs);

        putResource(materialize, name, "materialize-rule");
    }

    private static void hasCast(Instance movie, Instance person, Instance character) {
        hasCast.addRelation()
                .addRolePlayer(productionWithCast, movie)
                .addRolePlayer(actor, person)
                .addRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Instance movie, Instance genre) {
        hasGenre.addRelation()
                .addRolePlayer(productionWithGenre, movie)
                .addRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Instance cluster, Instance... movies) {
        Relation relation = hasCluster.addRelation().addRolePlayer(clusterOfProduction, cluster);
        for (Instance movie : movies) {
            relation.addRolePlayer(productionWithCluster, movie);
        }
    }
}

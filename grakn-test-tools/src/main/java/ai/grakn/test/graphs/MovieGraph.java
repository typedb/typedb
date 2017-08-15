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

package ai.grakn.test.graphs;

import ai.grakn.GraknGraph;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Thing;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.ResourceType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.RuleType;
import ai.grakn.graql.Pattern;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.function.Consumer;

/**
 *
 * @author fppt, Felix Chapman
 */
public class MovieGraph extends TestGraph {

    private static EntityType production, movie, person, genre, character, cluster, language;
    private static ResourceType<String> title, gender, realName, name;
    private static ResourceType<Long> tmdbVoteCount, runtime;
    private static ResourceType<Double> tmdbVoteAverage;
    private static ResourceType<LocalDateTime> releaseDate;
    private static RelationType hasCast, authoredBy, directedBy, hasGenre, hasCluster;
    private static Role productionBeingDirected, director, productionWithCast, actor, characterBeingPlayed;
    private static Role genreOfProduction, productionWithGenre, clusterOfProduction, productionWithCluster;
    private static Role work, author;
    private static RuleType aRuleType;

    private static Thing godfather, theMuppets, heat, apocalypseNow, hocusPocus, spy, chineseCoffee;
    private static Thing marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw;
    private static Thing mirandaHeart, betteMidler, sarahJessicaParker;
    private static Thing crime, drama, war, action, comedy, family, musical, fantasy;
    private static Thing donVitoCorleone, michaelCorleone, colonelWalterEKurtz, benjaminLWillard, ltVincentHanna;
    private static Thing neilMcCauley, bradleyFine, nancyBArtingstall, winifred, sarah, harry;
    private static Thing cluster0, cluster1;

    public static Consumer<GraknGraph> get(){
        return new MovieGraph().build();
    }

    @Override
    public void buildOntology(GraknGraph graph) {
        work = graph.putRole("work");
        author = graph.putRole("author");
        authoredBy = graph.putRelationType("authored-by").relates(work).relates(author);

        productionBeingDirected = graph.putRole("production-being-directed").sup(work);
        director = graph.putRole("director").sup(author);
        directedBy = graph.putRelationType("directed-by").sup(authoredBy)
                .relates(productionBeingDirected).relates(director);

        productionWithCast = graph.putRole("production-with-cast");
        actor = graph.putRole("actor");
        characterBeingPlayed = graph.putRole("character-being-played");
        hasCast = graph.putRelationType("has-cast")
                .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);

        genreOfProduction = graph.putRole("genre-of-production");
        productionWithGenre = graph.putRole("production-with-genre");
        hasGenre = graph.putRelationType("has-genre")
                .relates(genreOfProduction).relates(productionWithGenre);

        clusterOfProduction = graph.putRole("cluster-of-production");
        productionWithCluster = graph.putRole("production-with-cluster");
        hasCluster = graph.putRelationType("has-cluster")
                .relates(clusterOfProduction).relates(productionWithCluster);

        title = graph.putResourceType("title", ResourceType.DataType.STRING);
        title.resource(title);

        tmdbVoteCount = graph.putResourceType("tmdb-vote-count", ResourceType.DataType.LONG);
        tmdbVoteAverage = graph.putResourceType("tmdb-vote-average", ResourceType.DataType.DOUBLE);
        releaseDate = graph.putResourceType("release-date", ResourceType.DataType.DATE);
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

        movie = graph.putEntityType("movie").sup(production);

        graph.putEntityType("tv-show").sup(production);

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
        putResource(godfather, releaseDate, LocalDate.of(1984, 1, 1).atStartOfDay());

        theMuppets = movie.addEntity();
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, LocalDate.of(1985, 2, 2).atStartOfDay());

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
        putResource(spy, releaseDate, LocalDate.of(1986, 3, 3).atStartOfDay());

        chineseCoffee = movie.addEntity();
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, LocalDate.of(2000, 9, 2).atStartOfDay());

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

        Pattern when = graph.graql().parsePattern("$x plays actor");
        Pattern then = graph.graql().parsePattern("$x isa person");

        Rule expectation = aRuleType.putRule(when, then);

        putResource(expectation, name, "expectation-rule");

        when = graph.graql().parsePattern("$x has name 'materialize-when'");
        then = graph.graql().parsePattern("$x has name 'materialize-then'");
        Rule materialize = aRuleType.putRule(when, then);

        putResource(materialize, name, "materialize-rule");
    }

    private static void hasCast(Thing movie, Thing person, Thing character) {
        hasCast.addRelation()
                .addRolePlayer(productionWithCast, movie)
                .addRolePlayer(actor, person)
                .addRolePlayer(characterBeingPlayed, character);
    }

    private static void hasGenre(Thing movie, Thing genre) {
        hasGenre.addRelation()
                .addRolePlayer(productionWithGenre, movie)
                .addRolePlayer(genreOfProduction, genre);
    }

    private static void hasCluster(Thing cluster, Thing... movies) {
        Relationship relationship = hasCluster.addRelation().addRolePlayer(clusterOfProduction, cluster);
        for (Thing movie : movies) {
            relationship.addRolePlayer(productionWithCluster, movie);
        }
    }
}

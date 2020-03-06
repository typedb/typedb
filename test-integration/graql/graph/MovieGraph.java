/*
 * Copyright (C) 2020 Grakn Labs
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.graph;

import grakn.core.core.Schema;
import grakn.core.kb.concept.api.Attribute;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Relation;
import grakn.core.kb.concept.api.RelationType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import graql.lang.Graql;
import graql.lang.pattern.Pattern;

import java.time.LocalDate;
import java.time.LocalDateTime;

@SuppressWarnings("Duplicates")
public class MovieGraph {

    private static EntityType production, movie, person, genre, character, cluster, language;
    private static AttributeType<String> title, gender, realName, name, provenance;
    private static AttributeType<Long> tmdbVoteCount, runtime;
    private static AttributeType<Double> tmdbVoteAverage;
    private static AttributeType<LocalDateTime> releaseDate;
    private static RelationType hasCast, authoredBy, directedBy, hasGenre, hasCluster;
    private static Role productionBeingDirected, director, productionWithCast, actor, characterBeingPlayed;
    private static Role genreOfProduction, productionWithGenre, clusterOfProduction, productionWithCluster;
    private static Role work, author;

    private static Thing godfather, theMuppets, heat, apocalypseNow, hocusPocus, spy, chineseCoffee;
    private static Thing marlonBrando, alPacino, missPiggy, kermitTheFrog, martinSheen, robertDeNiro, judeLaw;
    private static Thing mirandaHeart, betteMidler, sarahJessicaParker;
    private static Thing crime, drama, war, action, comedy, family, musical, fantasy;
    private static Thing donVitoCorleone, michaelCorleone, colonelWalterEKurtz, benjaminLWillard, ltVincentHanna;
    private static Thing neilMcCauley, bradleyFine, nancyBArtingstall, winifred, sarah, harry;
    private static Thing cluster0, cluster1;


    public static void load(Session session) {
        try (Transaction transaction = session.writeTransaction()) {
            buildSchema(transaction);
            buildInstances(transaction);
            buildRelations();
            buildRules(transaction);

            transaction.commit();
        }
    }


    private static <T> void putResource(Thing thing, AttributeType<T> attributeType, T resource) {
        Attribute attributeInstance = attributeType.create(resource);
        thing.has(attributeInstance);
    }


    private static void buildSchema(Transaction tx) {

        tmdbVoteCount = tx.putAttributeType("tmdb-vote-count", AttributeType.DataType.LONG);
        tmdbVoteAverage = tx.putAttributeType("tmdb-vote-average", AttributeType.DataType.DOUBLE);
        releaseDate = tx.putAttributeType("release-date", AttributeType.DataType.DATE);
        runtime = tx.putAttributeType("runtime", AttributeType.DataType.LONG);
        gender = tx.putAttributeType("gender", AttributeType.DataType.STRING).regex("(fe)?male");
        realName = tx.putAttributeType("real-name", AttributeType.DataType.STRING);
        name = tx.putAttributeType("name", AttributeType.DataType.STRING);
        provenance = tx.putAttributeType("provenance", AttributeType.DataType.STRING);

        work = tx.putRole("work");
        author = tx.putRole("author");
        authoredBy = tx.putRelationType("authored-by").relates(work).relates(author);

        productionBeingDirected = tx.putRole("production-being-directed").sup(work);
        director = tx.putRole("director").sup(author);
        directedBy = tx.putRelationType("directed-by").sup(authoredBy)
                .relates(productionBeingDirected).relates(director);

        productionWithCast = tx.putRole("production-with-cast");
        actor = tx.putRole("actor");
        characterBeingPlayed = tx.putRole("character-being-played");
        hasCast = tx.putRelationType("has-cast")
                .relates(productionWithCast).relates(actor).relates(characterBeingPlayed);

        genreOfProduction = tx.putRole("genre-of-production");
        productionWithGenre = tx.putRole("production-with-genre");
        hasGenre = tx.putRelationType("has-genre")
                .relates(genreOfProduction).relates(productionWithGenre);

        clusterOfProduction = tx.putRole("cluster-of-production");
        productionWithCluster = tx.putRole("production-with-cluster");
        hasCluster = tx.putRelationType("has-cluster")
                .relates(clusterOfProduction).relates(productionWithCluster);

        title = tx.putAttributeType("title", AttributeType.DataType.STRING);
        title.has(title);

        production = tx.putEntityType("production")
                .plays(productionWithCluster).plays(productionBeingDirected).plays(productionWithCast).plays(work)
                .plays(productionWithGenre);

        production.has(title);
        production.has(tmdbVoteCount);
        production.has(tmdbVoteAverage);
        production.has(releaseDate);
        production.has(runtime);

        movie = tx.putEntityType("movie").sup(production);

        tx.putEntityType("tv-show").sup(production);

        person = tx.putEntityType("person")
                .plays(director).plays(actor).plays(characterBeingPlayed).plays(author);

        person.has(gender);
        person.has(name);
        person.has(realName);

        genre = tx.putEntityType("genre").plays(genreOfProduction);
        genre.key(name);

        character = tx.putEntityType("character")
                .plays(characterBeingPlayed);

        character.has(name);

        tx.putEntityType("award");
        language = tx.putEntityType("language");

        language.has(name);

        cluster = tx.putEntityType("cluster").plays(clusterOfProduction);
        cluster.has(name);

        tx.getType(Schema.ImplicitType.HAS.getLabel("title")).has(provenance);
        authoredBy.has(provenance);
    }

    private static void buildInstances(Transaction tx) {
        godfather = movie.create();
        putResource(godfather, title, "Godfather");
        putResource(godfather, tmdbVoteCount, 1000L);
        putResource(godfather, tmdbVoteAverage, 8.6);
        putResource(godfather, releaseDate, LocalDate.of(1984, 1, 1).atStartOfDay());

        theMuppets = movie.create();
        putResource(theMuppets, title, "The Muppets");
        putResource(theMuppets, tmdbVoteCount, 100L);
        putResource(theMuppets, tmdbVoteAverage, 7.6);
        putResource(theMuppets, releaseDate, LocalDate.of(1985, 2, 2).atStartOfDay());

        apocalypseNow = movie.create();
        putResource(apocalypseNow, title, "Apocalypse Now");
        putResource(apocalypseNow, tmdbVoteCount, 400L);
        putResource(apocalypseNow, tmdbVoteAverage, 8.4);

        heat = movie.create();
        putResource(heat, title, "Heat");

        hocusPocus = movie.create();
        putResource(hocusPocus, title, "Hocus Pocus");
        putResource(hocusPocus, tmdbVoteCount, 435L);

        spy = movie.create();
        putResource(spy, title, "Spy");
        putResource(spy, releaseDate, LocalDate.of(1986, 3, 3).atStartOfDay());

        chineseCoffee = movie.create();
        putResource(chineseCoffee, title, "Chinese Coffee");
        putResource(chineseCoffee, tmdbVoteCount, 5L);
        putResource(chineseCoffee, tmdbVoteAverage, 3.1d);
        putResource(chineseCoffee, releaseDate, LocalDate.of(2000, 9, 2).atStartOfDay());

        marlonBrando = person.create();
        putResource(marlonBrando, name, "Marlon Brando");
        alPacino = person.create();
        putResource(alPacino, name, "Al Pacino");
        missPiggy = person.create();
        putResource(missPiggy, name, "Miss Piggy");
        kermitTheFrog = person.create();
        putResource(kermitTheFrog, name, "Kermit The Frog");
        martinSheen = person.create();
        putResource(martinSheen, name, "Martin Sheen");
        robertDeNiro = person.create();
        putResource(robertDeNiro, name, "Robert de Niro");
        judeLaw = person.create();
        putResource(judeLaw, name, "Jude Law");
        mirandaHeart = person.create();
        putResource(mirandaHeart, name, "Miranda Heart");
        betteMidler = person.create();
        putResource(betteMidler, name, "Bette Midler");
        sarahJessicaParker = person.create();
        putResource(sarahJessicaParker, name, "Sarah Jessica Parker");

        crime = genre.create();
        putResource(crime, name, "crime");
        drama = genre.create();
        putResource(drama, name, "drama");
        war = genre.create();
        putResource(war, name, "war");
        action = genre.create();
        putResource(action, name, "action");
        comedy = genre.create();
        putResource(comedy, name, "comedy");
        family = genre.create();
        putResource(family, name, "family");
        musical = genre.create();
        putResource(musical, name, "musical");
        fantasy = genre.create();
        putResource(fantasy, name, "fantasy");

        donVitoCorleone = character.create();
        putResource(donVitoCorleone, name, "Don Vito Corleone");
        michaelCorleone = character.create();
        putResource(michaelCorleone, name, "Michael Corleone");
        colonelWalterEKurtz = character.create();
        putResource(colonelWalterEKurtz, name, "Colonel Walter E. Kurtz");
        benjaminLWillard = character.create();
        putResource(benjaminLWillard, name, "Benjamin L. Willard");
        ltVincentHanna = character.create();
        putResource(ltVincentHanna, name, "Lt Vincent Hanna");
        neilMcCauley = character.create();
        putResource(neilMcCauley, name, "Neil McCauley");
        bradleyFine = character.create();
        putResource(bradleyFine, name, "Bradley Fine");
        nancyBArtingstall = character.create();
        putResource(nancyBArtingstall, name, "Nancy B Artingstall");
        winifred = character.create();
        putResource(winifred, name, "Winifred");
        sarah = character.create();
        putResource(sarah, name, "Sarah");
        harry = character.create();
        putResource(harry, name, "Harry");

        cluster0 = cluster.create();
        cluster1 = cluster.create();
        putResource(cluster0, name, "0");
        putResource(cluster1, name, "1");
    }

    private static void buildRelations() {
        directedBy.create()
                .assign(productionBeingDirected, chineseCoffee)
                .assign(director, alPacino);

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

    private static void buildRules(Transaction tx) {
        // These rules are totally made up for testing purposes and don't work!
        Pattern when = Graql.parsePattern("$x has name 'expectation-when';");
        Pattern then = Graql.parsePattern("$x has name 'expectation-then';");

        tx.putRule("expectation-rule", when, then);

        when = Graql.parsePattern("$x has name 'materialize-when';");
        then = Graql.parsePattern("$x has name 'materialize-then';");
        tx.putRule("materialize-rule", when, then);
    }

    private static void hasCast(Thing movie, Thing person, Thing character) {
        hasCast.create()
                .assign(productionWithCast, movie)
                .assign(actor, person)
                .assign(characterBeingPlayed, character);
    }

    private static void hasGenre(Thing movie, Thing genre) {
        hasGenre.create()
                .assign(productionWithGenre, movie)
                .assign(genreOfProduction, genre);
    }

    private static void hasCluster(Thing cluster, Thing... movies) {
        Relation relation = hasCluster.create().assign(clusterOfProduction, cluster);
        for (Thing movie : movies) {
            relation.assign(productionWithCluster, movie);
        }
    }
}

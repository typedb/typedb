/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.matcher;

import com.google.common.collect.ImmutableSet;
import org.hamcrest.Matcher;

import static ai.grakn.util.Schema.ImplicitType.HAS;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Matchers for use with the movie dataset.
 *
 * @author Felix Chapman
 */
public class MovieMatchers {

    public static final Matcher<MatchableConcept> production = GraknMatchers.type("production");
    public static final Matcher<MatchableConcept> movie = GraknMatchers.type("movie");
    public static final Matcher<MatchableConcept> person = GraknMatchers.type("person");
    public static final Matcher<MatchableConcept> genre = GraknMatchers.type("genre");
    public static final Matcher<MatchableConcept> character = GraknMatchers.type("character");
    public static final Matcher<MatchableConcept> cluster = GraknMatchers.type("cluster");
    public static final Matcher<MatchableConcept> language = GraknMatchers.type("language");
    public static final Matcher<MatchableConcept> title = GraknMatchers.type("title");
    public static final Matcher<MatchableConcept> gender = GraknMatchers.type("gender");
    public static final Matcher<MatchableConcept> realName = GraknMatchers.type("real-name");
    public static final Matcher<MatchableConcept> name = GraknMatchers.type("name");
    public static final Matcher<MatchableConcept> provenance = GraknMatchers.type("provenance");
    public static final Matcher<MatchableConcept> tmdbVoteCount = GraknMatchers.type("tmdb-vote-count");
    public static final Matcher<MatchableConcept> releaseDate = GraknMatchers.type("release-date");
    public static final Matcher<MatchableConcept> runtime = GraknMatchers.type("runtime");
    public static final Matcher<MatchableConcept> tmdbVoteAverage = GraknMatchers.type("tmdb-vote-average");
    public static final Matcher<MatchableConcept> genreOfProduction = GraknMatchers.role("genre-of-production");
    public static final Matcher<MatchableConcept> keyNameOwner = GraknMatchers.role("@key-name-owner");
    public static final Matcher<MatchableConcept> materializeRule = GraknMatchers.rule("materialize-rule");
    public static final Matcher<MatchableConcept> expectationRule = GraknMatchers.rule("expectation-rule");
    public static final Matcher<MatchableConcept> hasTitle = GraknMatchers.type(HAS.getLabel("title"));
    public static final Matcher<MatchableConcept> godfather = GraknMatchers.instance("Godfather");
    public static final Matcher<MatchableConcept> theMuppets = GraknMatchers.instance("The Muppets");
    public static final Matcher<MatchableConcept> heat = GraknMatchers.instance("Heat");
    public static final Matcher<MatchableConcept> apocalypseNow = GraknMatchers.instance("Apocalypse Now");
    public static final Matcher<MatchableConcept> hocusPocus = GraknMatchers.instance("Hocus Pocus");
    public static final Matcher<MatchableConcept> spy = GraknMatchers.instance("Spy");
    public static final Matcher<MatchableConcept> chineseCoffee = GraknMatchers.instance("Chinese Coffee");
    public static final Matcher<MatchableConcept> marlonBrando = GraknMatchers.instance("Marlon Brando");
    public static final Matcher<MatchableConcept> alPacino = GraknMatchers.instance("Al Pacino");
    public static final Matcher<MatchableConcept> missPiggy = GraknMatchers.instance("Miss Piggy");
    public static final Matcher<MatchableConcept> kermitTheFrog = GraknMatchers.instance("Kermit The Frog");
    public static final Matcher<MatchableConcept> martinSheen = GraknMatchers.instance("Martin Sheen");
    public static final Matcher<MatchableConcept> robertDeNiro = GraknMatchers.instance("Robert de Niro");
    public static final Matcher<MatchableConcept> judeLaw = GraknMatchers.instance("Jude Law");
    public static final Matcher<MatchableConcept> mirandaHeart = GraknMatchers.instance("Miranda Heart");
    public static final Matcher<MatchableConcept> betteMidler = GraknMatchers.instance("Bette Midler");
    public static final Matcher<MatchableConcept> sarahJessicaParker = GraknMatchers.instance("Sarah Jessica Parker");
    public static final Matcher<MatchableConcept> crime = GraknMatchers.instance("crime");
    public static final Matcher<MatchableConcept> drama = GraknMatchers.instance("drama");
    public static final Matcher<MatchableConcept> war = GraknMatchers.instance("war");
    public static final Matcher<MatchableConcept> action = GraknMatchers.instance("action");
    public static final Matcher<MatchableConcept> comedy = GraknMatchers.instance("comedy");
    public static final Matcher<MatchableConcept> family = GraknMatchers.instance("family");
    public static final Matcher<MatchableConcept> musical = GraknMatchers.instance("musical");
    public static final Matcher<MatchableConcept> fantasy = GraknMatchers.instance("fantasy");
    public static final Matcher<MatchableConcept> benjaminLWillard = GraknMatchers.instance("Benjamin L. Willard");
    public static final Matcher<MatchableConcept> neilMcCauley = GraknMatchers.instance("Neil McCauley");
    public static final Matcher<MatchableConcept> sarah = GraknMatchers.instance("Sarah");
    public static final Matcher<MatchableConcept> harry = GraknMatchers.instance("Harry");

    @SuppressWarnings("unchecked")
    public static final ImmutableSet<Matcher<? super MatchableConcept>> movies = ImmutableSet.of(
            godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee
    );

    public static final Matcher<Iterable<? extends MatchableConcept>> containsAllMovies = containsInAnyOrder(movies);
}

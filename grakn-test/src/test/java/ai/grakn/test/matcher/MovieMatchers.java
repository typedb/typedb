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
 *
 */

package ai.grakn.test.matcher;

import com.google.common.collect.ImmutableSet;
import org.hamcrest.Matcher;

import static ai.grakn.test.matcher.GraknMatchers.instance;
import static ai.grakn.test.matcher.GraknMatchers.type;
import static ai.grakn.util.Schema.ImplicitType.HAS;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Matchers for use with the movie dataset.
 */
public class MovieMatchers {

    public static final Matcher<MatchableConcept> production = type("production");
    public static final Matcher<MatchableConcept> movie = type("movie");
    public static final Matcher<MatchableConcept> person = type("person");
    public static final Matcher<MatchableConcept> genre = type("genre");
    public static final Matcher<MatchableConcept> character = type("character");
    public static final Matcher<MatchableConcept> cluster = type("cluster");
    public static final Matcher<MatchableConcept> language = type("language");
    public static final Matcher<MatchableConcept> title = type("title");
    public static final Matcher<MatchableConcept> gender = type("gender");
    public static final Matcher<MatchableConcept> realName = type("real-name");
    public static final Matcher<MatchableConcept> name = type("name");
    public static final Matcher<MatchableConcept> tmdbVoteCount = type("tmdb-vote-count");
    public static final Matcher<MatchableConcept> releaseDate = type("release-date");
    public static final Matcher<MatchableConcept> runtime = type("runtime");
    public static final Matcher<MatchableConcept> tmdbVoteAverage = type("tmdb-vote-average");
    public static final Matcher<MatchableConcept> genreOfProduction = type("genre-of-production");
    public static final Matcher<MatchableConcept> aRuleType = type("a-rule-type");
    public static final Matcher<MatchableConcept> hasTitle = type(HAS.getLabel("title"));
    public static final Matcher<MatchableConcept> godfather = instance("Godfather");
    public static final Matcher<MatchableConcept> theMuppets = instance("The Muppets");
    public static final Matcher<MatchableConcept> heat = instance("Heat");
    public static final Matcher<MatchableConcept> apocalypseNow = instance("Apocalypse Now");
    public static final Matcher<MatchableConcept> hocusPocus = instance("Hocus Pocus");
    public static final Matcher<MatchableConcept> spy = instance("Spy");
    public static final Matcher<MatchableConcept> chineseCoffee = instance("Chinese Coffee");
    public static final Matcher<MatchableConcept> marlonBrando = instance("Marlon Brando");
    public static final Matcher<MatchableConcept> alPacino = instance("Al Pacino");
    public static final Matcher<MatchableConcept> missPiggy = instance("Miss Piggy");
    public static final Matcher<MatchableConcept> kermitTheFrog = instance("Kermit The Frog");
    public static final Matcher<MatchableConcept> martinSheen = instance("Martin Sheen");
    public static final Matcher<MatchableConcept> robertDeNiro = instance("Robert de Niro");
    public static final Matcher<MatchableConcept> judeLaw = instance("Jude Law");
    public static final Matcher<MatchableConcept> mirandaHeart = instance("Miranda Heart");
    public static final Matcher<MatchableConcept> betteMidler = instance("Bette Midler");
    public static final Matcher<MatchableConcept> sarahJessicaParker = instance("Sarah Jessica Parker");
    public static final Matcher<MatchableConcept> crime = instance("crime");
    public static final Matcher<MatchableConcept> drama = instance("drama");
    public static final Matcher<MatchableConcept> war = instance("war");
    public static final Matcher<MatchableConcept> action = instance("action");
    public static final Matcher<MatchableConcept> comedy = instance("comedy");
    public static final Matcher<MatchableConcept> family = instance("family");
    public static final Matcher<MatchableConcept> musical = instance("musical");
    public static final Matcher<MatchableConcept> fantasy = instance("fantasy");
    public static final Matcher<MatchableConcept> benjaminLWillard = instance("Benjamin L. Willard");
    public static final Matcher<MatchableConcept> neilMcCauley = instance("Neil McCauley");
    public static final Matcher<MatchableConcept> sarah = instance("Sarah");
    public static final Matcher<MatchableConcept> harry = instance("Harry");

    @SuppressWarnings("unchecked")
    public static final ImmutableSet<Matcher<? super MatchableConcept>> movies = ImmutableSet.of(
            godfather, theMuppets, apocalypseNow, heat, hocusPocus, spy, chineseCoffee
    );

    public static final Matcher<Iterable<? extends MatchableConcept>> containsAllMovies = containsInAnyOrder(movies);
}

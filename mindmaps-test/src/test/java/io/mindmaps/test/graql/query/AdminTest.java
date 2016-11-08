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

package io.mindmaps.test.graql.query;

import com.google.common.collect.Sets;
import io.mindmaps.concept.Type;
import io.mindmaps.graql.DeleteQuery;
import io.mindmaps.graql.InsertQuery;
import io.mindmaps.graql.MatchQuery;
import io.mindmaps.graql.QueryBuilder;
import io.mindmaps.graql.admin.Conjunction;
import io.mindmaps.graql.admin.PatternAdmin;
import io.mindmaps.test.AbstractMovieGraphTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static io.mindmaps.graql.Graql.id;
import static io.mindmaps.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AdminTest extends AbstractMovieGraphTest {

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = graph.graql();
    }

    @Test
    public void testGetTypesInQuery() {
        MatchQuery query = qb.match(
                var("x").isa(id("movie").sub("production")).has("tmdb-vote-count", 400),
                var("y").isa("character").id("123"),
                var().rel("production-with-cast", "x").rel("y").isa("has-cast")
        );

        Set<Type> types = Stream.of(
                "movie", "production", "tmdb-vote-count", "character", "production-with-cast", "has-cast"
        ).map(graph::getType).collect(toSet());

        assertEquals(types, query.admin().getTypes());
    }

    @Test
    public void testDefaultGetSelectedNamesInQuery() {
        MatchQuery query = qb.match(var("x").isa(var("y")));

        assertEquals(Sets.newHashSet("x", "y"), query.admin().getSelectedNames());
    }

    @Test
    public void testExplicitGetSelectedNamesInQuery() {
        MatchQuery query = qb.match(var("x").isa(var("y"))).select("x");

        assertEquals(Sets.newHashSet("x"), query.admin().getSelectedNames());
    }

    @Test
    public void testGetPatternInQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"), var("x").value("Bob"));

        Conjunction<PatternAdmin> conjunction = query.admin().getPattern();
        assertNotNull(conjunction);

        Set<PatternAdmin> patterns = conjunction.getPatterns();
        assertEquals(2, patterns.size());
    }

    @Test
    public void testMutateMatchQuery() {
        MatchQuery query = qb.match(var("x").isa("movie"));

        Conjunction<PatternAdmin> pattern = query.admin().getPattern();
        pattern.getPatterns().add(var("x").has("title", "Spy").admin());

        assertEquals(1, query.stream().count());
    }

    @Test
    public void testInsertQueryMatchPatternEmpty() {
        InsertQuery query = qb.insert(var().id("123").isa("movie"));
        assertFalse(query.admin().getMatchQuery().isPresent());
    }

    @Test
    public void testInsertQueryWithMatchQuery() {
        InsertQuery query = qb.match(var("x").isa("movie")).insert(var().id("123").isa("movie"));
        assertEquals(Optional.of("match $x isa movie;"), query.admin().getMatchQuery().map(Object::toString));
    }

    @Test
    public void testInsertQueryGetVars() {
        InsertQuery query = qb.insert(var().id("123").isa("movie"), var().id("123").value("Hi"));
        // Should not merge variables
        assertEquals(2, query.admin().getVars().size());
    }

    @Test
    public void testDeleteQueryDeleters() {
        DeleteQuery query = qb.match(var("x").isa("movie")).delete("x");
        assertEquals(1, query.admin().getDeleters().size());
    }

    @Test
    public void testDeleteQueryPattern() {
        DeleteQuery query = qb.match(var("x").isa("movie")).delete("x");
        assertEquals("match $x isa movie;", query.admin().getMatchQuery().toString());
    }

    @Test
    public void testInsertQueryGetTypes() {
        InsertQuery query = qb.insert(var("x").isa("person").has("name"), var().rel("actor", "x").isa("has-cast"));
        Set<Type> types = Stream.of("person", "name", "actor", "has-cast").map(graph::getType).collect(toSet());
        assertEquals(types, query.admin().getTypes());
    }

    @Test
    public void testMatchInsertQueryGetTypes() {
        InsertQuery query = qb.match(var("y").isa("movie"))
                        .insert(var("x").isa("person").has("name"), var().rel("actor", "x").isa("has-cast"));

        Set<Type> types =
                Stream.of("movie", "person", "name", "actor", "has-cast").map(graph::getType).collect(toSet());

        assertEquals(types, query.admin().getTypes());
    }
}

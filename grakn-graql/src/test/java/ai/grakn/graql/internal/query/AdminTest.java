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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Label;
import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Match;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.test.rule.SampleKBContext;
import ai.grakn.test.kbs.MovieKB;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.Graql.label;
import static ai.grakn.graql.Graql.var;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AdminTest {

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testGetTypesInQuery() {
        Match match = qb.match(
                var("x").isa(label("movie").sub("production")).has("tmdb-vote-count", 400),
                var("y").isa("character"),
                var().rel("production-with-cast", "x").rel("y").isa("has-cast")
        );

        Set<SchemaConcept> types = Stream.of(
                "movie", "production", "tmdb-vote-count", "character", "production-with-cast", "has-cast"
        ).map(t -> rule.tx().<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());

        assertEquals(types, match.admin().getSchemaConcepts());
    }

    @Test
    public void testDefaultGetSelectedNamesInQuery() {
        Match match = qb.match(var("x").isa(var("y")));

        assertEquals(Sets.newHashSet(Graql.var("x"), Graql.var("y")), match.admin().getSelectedNames());
    }

    @Test
    public void testGetPatternInQuery() {
        Match match = qb.match(var("x").isa("movie"), var("x").val("Bob"));

        Conjunction<PatternAdmin> conjunction = match.admin().getPattern();
        assertNotNull(conjunction);

        Set<PatternAdmin> patterns = conjunction.getPatterns();
        assertEquals(2, patterns.size());
    }

    @Test
    public void testMutateMatch() {
        Match match = qb.match(var("x").isa("movie"));

        Conjunction<PatternAdmin> pattern = match.admin().getPattern();
        pattern.getPatterns().add(var("x").has("title", "Spy").admin());

        assertEquals(1, match.stream().count());
    }

    @Test
    public void testInsertQueryMatchPatternEmpty() {
        InsertQuery query = qb.insert(var().id(ConceptId.of("123")).isa("movie"));
        assertFalse(query.admin().match().isPresent());
    }

    @Test
    public void testInsertQueryWithMatch() {
        InsertQuery query = qb.match(var("x").isa("movie")).insert(var().id(ConceptId.of("123")).isa("movie"));
        assertEquals(Optional.of("match $x isa movie;"), query.admin().match().map(Object::toString));
    }

    @Test
    public void testInsertQueryGetVars() {
        InsertQuery query = qb.insert(var().id(ConceptId.of("123")).isa("movie"), var().id(ConceptId.of("123")).val("Hi"));
        // Should not merge variables
        assertEquals(2, query.admin().varPatterns().size());
    }

    @Test
    public void testDeleteQueryPattern() {
        DeleteQuery query = qb.match(var("x").isa("movie")).delete("x");
        assertEquals("match $x isa movie;", query.admin().match().toString());
    }

    @Test
    public void testInsertQueryGetTypes() {
        InsertQuery query = qb.insert(var("x").isa("person").has("name", var("y")), var().rel("actor", "x").isa("has-cast"));
        Set<SchemaConcept> types = Stream.of("person", "name", "actor", "has-cast").map(t -> rule.tx().<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());
        assertEquals(types, query.admin().getSchemaConcepts());
    }

    @Test
    public void testMatchInsertQueryGetTypes() {
        InsertQuery query = qb.match(var("y").isa("movie"))
                        .insert(var("x").isa("person").has("name", var("z")), var().rel("actor", "x").isa("has-cast"));

        Set<SchemaConcept> types =
                Stream.of("movie", "person", "name", "actor", "has-cast").map(t -> rule.tx().<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());

        assertEquals(types, query.admin().getSchemaConcepts());
    }
}

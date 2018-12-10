/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.query;

import com.google.common.collect.Sets;
import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Label;
import grakn.core.graql.concept.SchemaConcept;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.query.pattern.Pattern.label;
import static grakn.core.graql.query.pattern.Pattern.var;
import static java.util.stream.Collectors.toSet;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class QueryAdminIT {

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;
    private QueryBuilder qb;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.transaction(Transaction.Type.WRITE);
        qb = tx.graql();
    }

    @After
    public void closeTransaction() {
        tx.close();
    }

    @AfterClass
    public static void closeSession() {
        session.close();
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
        ).map(t -> tx.<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());

        assertEquals(types, match.admin().getSchemaConcepts());
    }

    @Test
    public void testDefaultGetSelectedNamesInQuery() {
        Match match = qb.match(var("x").isa(var("y")));

        assertEquals(Sets.newHashSet(var("x"), var("y")), match.admin().getSelectedNames());
    }

    @Test
    public void testGetPatternInQuery() {
        Match match = qb.match(var("x").isa("movie"), var("x").val("Bob"));

        Conjunction<Pattern> conjunction = match.admin().getPatterns();
        assertNotNull(conjunction);

        Set<Pattern> patterns = conjunction.getPatterns();
        assertEquals(2, patterns.size());
    }

    @Test
    public void testMutateMatch() {
        Match match = qb.match(var("x").isa("movie"));

        Conjunction<Pattern> pattern = match.admin().getPatterns();
        pattern.getPatterns().add(var("x").has("title", "Spy"));

        assertEquals(1, match.stream().count());
    }

    @Test
    public void testInsertQueryMatchPatternEmpty() {
        InsertQuery query = qb.insert(var().id(ConceptId.of("123")).isa("movie"));
        assertNull(query.admin().match());
    }

    @Test
    public void testInsertQueryWithMatch() {
        InsertQuery query = qb.match(var("x").isa("movie")).insert(var().id(ConceptId.of("123")).isa("movie"));
        assertEquals("match $x isa movie;", query.admin().match().toString());

        query = qb.match(var("x").isaExplicit("movie")).insert(var().id(ConceptId.of("123")).isa("movie"));
        assertEquals("match $x isa! movie;", query.admin().match().toString());
    }

    @Test
    public void testInsertQueryGetVars() {
        InsertQuery query = qb.insert(var().id(ConceptId.of("123")).isa("movie"), var().id(ConceptId.of("123")).val("Hi"));
        // Should not merge variables
        assertEquals(2, query.admin().statements().size());
    }

    @Test
    public void testDeleteQueryPattern() {
        DeleteQuery query = qb.match(var("x").isa("movie")).delete("x");
        assertEquals("match $x isa movie;", query.admin().match().toString());

        query = qb.match(var("x").isaExplicit("movie")).delete("x");
        assertEquals("match $x isa! movie;", query.admin().match().toString());
    }

    @Test
    public void testInsertQueryGetTypes() {
        InsertQuery query = qb.insert(var("x").isa("person").has("name", var("y")), var().rel("actor", "x").isa("has-cast"));
        Set<SchemaConcept> types = Stream.of("person", "name", "actor", "has-cast").map(t -> tx.<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());
        assertEquals(types, query.admin().getSchemaConcepts());
    }

    @Test
    public void testMatchInsertQueryGetTypes() {
        InsertQuery query = qb.match(var("y").isa("movie"))
                .insert(var("x").isa("person").has("name", var("z")), var().rel("actor", "x").isa("has-cast"));

        Set<SchemaConcept> types =
                Stream.of("movie", "person", "name", "actor", "has-cast").map(t -> tx.<SchemaConcept>getSchemaConcept(Label.of(t))).collect(toSet());

        assertEquals(types, query.admin().getSchemaConcepts());
    }
}

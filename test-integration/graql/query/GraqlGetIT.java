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

package grakn.core.graql.query;

import grakn.core.concept.answer.AnswerGroup;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.concept.answer.Numeric;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.Concept;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.api.EntityType;
import grakn.core.kb.concept.api.Role;
import grakn.core.kb.concept.api.Thing;
import grakn.core.kb.graql.exception.GraqlSemanticException;
import grakn.core.kb.server.Session;
import grakn.core.kb.server.Transaction;
import grakn.core.rule.GraknTestServer;
import graql.lang.Graql;
import graql.lang.exception.GraqlException;
import graql.lang.pattern.Pattern;
import graql.lang.query.GraqlGet;
import graql.lang.statement.StatementThing;
import graql.lang.statement.Variable;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static graql.lang.Graql.var;
import static graql.lang.exception.ErrorMessage.VARIABLE_OUT_OF_SCOPE;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Duplicates")
public class GraqlGetIT {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @ClassRule
    public static GraknTestServer graknServer = new GraknTestServer();
    private static Session session;
    private Transaction tx;

    @BeforeClass
    public static void newSession() {
        session = graknServer.sessionWithNewKeyspace();
        MovieGraph.load(session);
    }

    @Before
    public void newTransaction() {
        tx = session.writeTransaction();
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
    public void testGetSort() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y")
        );

        assertEquals("Al Pacino", answers.get(0).get("y").asAttribute().value());
        assertEquals("Bette Midler", answers.get(1).get("y").asAttribute().value());
        assertEquals("Jude Law", answers.get(2).get("y").asAttribute().value());
        assertEquals("Kermit The Frog", answers.get(3).get("y").asAttribute().value());
    }

    @Test
    public void testGetSortAscLimit() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y", "asc").limit(3)
        );

        assertEquals(3, answers.size());
        assertEquals("Al Pacino", answers.get(0).get("y").asAttribute().value());
        assertEquals("Bette Midler", answers.get(1).get("y").asAttribute().value());
        assertEquals("Jude Law", answers.get(2).get("y").asAttribute().value());
    }

    @Test
    public void testGetSortDesc() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y", "desc")
        );

        assertEquals(10, answers.size());
        assertEquals("Sarah Jessica Parker", answers.get(0).get("y").asAttribute().value());
        assertEquals("Robert de Niro", answers.get(1).get("y").asAttribute().value());
        assertEquals("Miss Piggy", answers.get(2).get("y").asAttribute().value());
        assertEquals("Miranda Heart", answers.get(3).get("y").asAttribute().value());
    }

    @Test
    public void testGetSortDescOffsetLimit() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("person").has("name", var("y")))
                        .get().sort("y", "desc").offset(3).limit(4)
        );

        assertEquals(4, answers.size());
        assertEquals("Miranda Heart", answers.get(0).get("y").asAttribute().value());
        assertEquals("Martin Sheen", answers.get(1).get("y").asAttribute().value());
        assertEquals("Marlon Brando", answers.get(2).get("y").asAttribute().value());
        assertEquals("Kermit The Frog", answers.get(3).get("y").asAttribute().value());
    }

    @Test
    public void testGetSortStringIgnoreCase() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("name")).get().sort("x").limit(5)
        );

        assertEquals(5, answers.size());
        assertEquals("0", answers.get(0).get("x").asAttribute().value());
        assertEquals("1", answers.get(1).get("x").asAttribute().value());
        assertEquals("action", answers.get(2).get("x").asAttribute().value());
        assertEquals("Al Pacino", answers.get(3).get("x").asAttribute().value());
        assertEquals("Benjamin L. Willard", answers.get(4).get("x").asAttribute().value());
    }

    @Test
    public void testGetContainsStringIgnoreCase() {
        List<ConceptMap> answers = tx.execute(
                Graql.match(var("x").isa("name").contains("jess")).get()
        );

        assertEquals(1, answers.size());
        assertEquals("Sarah Jessica Parker", answers.get(0).get("x").asAttribute().value());
    }

    @Test
    public void testCount() {
        List<Numeric> count = tx.execute(Graql.match(var("x").isa("movie"), var("y").isa("person"), var("r").rel("x").rel("y")).get().count());

        assertEquals(14, count.get(0).number().intValue());

        count = tx.execute(Graql.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).get("x").count());

        assertEquals(7, count.get(0).number().intValue());
    }

    @Test
    public void testGroup() {
        GraqlGet.Group groupQuery =
                Graql.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).get().group("x");
        List<AnswerGroup<ConceptMap>> groups = tx.execute(groupQuery);

        Assert.assertEquals(7, groups.size());

        groups.forEach(group -> {
            group.answers().forEach(conceptMap -> {
                assertEquals(group.owner(), conceptMap.get("x"));
                Assert.assertEquals(tx.getEntityType("person"), conceptMap.get("y").asThing().type());
            });
        });
    }

    @Test
    public void testGroupCount() {
        List<AnswerGroup<Numeric>> groupCount = tx.execute(Graql.match(var("x").isa("movie"), var("r").rel("x")).get()
                .group("x").count());
        Thing godfather = tx.getAttributeType("title").attribute("Godfather").owner();

        groupCount.forEach(group -> {
            if (group.owner().equals(Collections.singleton(godfather))) {
                assertEquals(9, group.answers().get(0).number().intValue());
            }
        });
    }

    @Test
    public void testGroupCountMultipleVars() {
        List<AnswerGroup<Numeric>> groupCounts = tx.execute(Graql.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("person"),
                var().rel("x").rel("y")
        ).get("x", "y", "z").group("x").count());

        Thing chineseCoffee = tx.getAttributeType("title").attribute("Chinese Coffee").owner();
        Thing godfather = tx.getAttributeType("title").attribute("Godfather").owner();

        boolean containsChineseCoffee = false, containsGodfather = false;

        for (AnswerGroup<Numeric> group : groupCounts) {
            if (group.owner().equals(chineseCoffee)) {
                assertEquals(10, group.answers().get(0).number().intValue());
                containsChineseCoffee = true;
            }
            if (group.owner().equals(godfather)) {
                assertEquals(20, group.answers().get(0).number().intValue());
                containsGodfather = true;
            }
        }

        assertTrue(containsChineseCoffee);
        assertTrue(containsGodfather);
    }

    @Test
    public void testGroupMax() {
        List<AnswerGroup<Numeric>> groupCount =
                tx.execute(Graql.match(
                        var("x").isa("person"),
                        var("y").isa("movie").has("tmdb-vote-count", var("z")),
                        var().rel("x").rel("y")
                ).get().group("x").max("z"));

        Thing marlonBrando = tx.getAttributeType("name").attribute("Marlon Brando").owner();
        Thing alPacino = tx.getAttributeType("name").attribute("Al Pacino").owner();

        boolean containsMarlonBrando = false, containsAlPacino = false;

        for (AnswerGroup<Numeric> group : groupCount) {
            if (group.owner().equals(marlonBrando)) {
                assertEquals(1000, group.answers().get(0).number().intValue());
                containsMarlonBrando = true;
            }
            if (group.owner().equals(alPacino)) {
                assertEquals(1000, group.answers().get(0).number().intValue());
                containsAlPacino = true;
            }
        }

        assertTrue(containsMarlonBrando);
        assertTrue(containsAlPacino);
    }

    @Test
    public void testSumInt() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count")).get()
                .sum("y");

        assertEquals(1940, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testSumDouble() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average")).get()
                .sum("y");

        assertEquals(27.7d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testSumFilteredAnswers() {
        GraqlGet.Aggregate query = Graql
                .match(var("x").isa("movie")
                               .has("tmdb-vote-count", var("y"))
                               .has("tmdb-vote-average", var("z")))
                .get("y", "z")
                .sum("z");

        assertEquals(tx.execute(query).get(0).number().doubleValue(), 27.7, 0.01d);
    }

    @Test
    public void testSumNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .sum("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMaxInt() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count")).get()
                .max("y");

        assertEquals(1000, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMaxDouble() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average")).get()
                .max("y");

        assertEquals(8.6d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMaxNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .max("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMinInt() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count")).get()
                .min("y");

        assertEquals(5, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMinNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .min("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMean() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average")).get()
                .mean("y");

        //noinspection OptionalGetWithoutIsPresent
        assertEquals((8.6d + 7.6d + 8.4d + 3.1d) / 4d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMeanNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .mean("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMedianInt() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count")).get()
                .median("y");

        assertEquals(400, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMedianDouble() {
        GraqlGet.Aggregate query = Graql.match(
                var("x").isa("movie"),
                var().rel("x").rel("y"),
                var("y").isa("tmdb-vote-average")
        ).get().median("y");

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(8.0d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMedianNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .median("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testStdDouble1() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie").has("tmdb-vote-count", var("y"))).get()
                .std("y");

        double mean = (1000d + 100d + 400d + 435d + 5d) / 5d;
        double variance = (pow(1000d - mean, 2d) + pow(100d - mean, 2d)
                + pow(400d - mean, 2d) + pow(435d - mean, 2d) + pow(5d - mean, 2d)) / 4d;
        double expected = sqrt(variance);

        assertEquals(expected, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdDouble2() {
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie").has("tmdb-vote-average", var("y"))).get()
                .std("y");

        double mean = (8.6d + 8.4d + 7.6d + 3.1d) / 4d;
        double variance =
                (pow(8.6d - mean, 2d) + pow(8.4d - mean, 2d) + pow(7.6d - mean, 2d) + pow(3.1d - mean, 2d)) / 3d;
        double expected = sqrt(variance);

        assertEquals(expected, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        GraqlGet.Aggregate query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random")).get()
                .std("y");

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testEmptyMatchCount() {
        assertEquals(0L, tx.execute(Graql.match(var().isa("runtime")).get().count()).get(0).number().longValue());
    }

    @Test
    public void testEmptyMatchThrows() {
        exception.expect(GraqlSemanticException.class);
        exception.expectMessage(Matchers.containsString("at least one property"));
        tx.execute(Graql.match(var()).get());
    }

    @Test(expected = Exception.class) // TODO: Would help if the error message is more specific
    public void testVarsNotExist() {
        tx.execute(Graql.match(var("x").isa("movie")).get().min("y"));
        System.out.println(tx.execute(Graql.match(var("x").isa("movie")).get().min("x")));
    }

    @Test(expected = Exception.class)
    public void testMinOnEntity() {
        tx.execute(Graql.match(var("x")).get().min("x"));
    }

    @Test(expected = Exception.class)
    public void testIncorrectResourceDataType() {
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).get().sum("y"));
    }

    @Test
    public void whenGroupVarIsNotInQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(VARIABLE_OUT_OF_SCOPE.getMessage(new Variable("z")));
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).get().group("z").count());
    }

    @Test
    public void whenSortVarIsNotInQuery_Throw() {
        exception.expect(GraqlException.class);
        exception.expectMessage(VARIABLE_OUT_OF_SCOPE.getMessage(new Variable("z")));
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).get().sort("z"));
    }


    @Test
    /**
     * This tests ensures that queries that match both vertices and edges in the query operate correctly
     */
    public void whenMatchingEdgeAndVertex_AnswerIsNotEmpty() {
        Role work = tx.putRole("work");
        EntityType somework = tx.putEntityType("somework").plays(work);
        Role author = tx.putRole("author");
        EntityType person = tx.putEntityType("person").plays(author);
        AttributeType<Long> year = tx.putAttributeType("year", AttributeType.DataType.LONG);
        tx.putRelationType("authored-by").relates(work).relates(author).has(year);

        Stream<ConceptMap> answers = tx.stream(Graql.parse("insert $x isa person;" +
                "$y isa somework; " +
                "$a isa year; $a 2020; " +
                "$r (author: $x, work: $y) isa authored-by; $r has year $a via $imp;").asInsert());

        List<ConceptId> insertedIds = answers.flatMap(conceptMap -> conceptMap.concepts().stream().map(Concept::id)).collect(Collectors.toList());
        tx.commit();
        newTransaction();

        List<Pattern> idPatterns = new ArrayList<>();
        for (int i = 0; i < insertedIds.size(); i++) {
            StatementThing id = var("v" + i).id(insertedIds.get(i).toString());
            idPatterns.add(id);
        }
        List<ConceptMap> answersById = tx.execute(Graql.match(idPatterns).get());
        assertEquals(answersById.size(), 1);

        // clean up, delete the IDs we inserted for this test
        tx.execute(Graql.match(idPatterns).delete(idPatterns.stream().flatMap(pattern -> pattern.variables().stream()).collect(Collectors.toList())));
        tx.commit();
    }
}

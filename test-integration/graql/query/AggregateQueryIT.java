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

import grakn.core.graql.answer.AnswerGroup;
import grakn.core.graql.answer.ConceptMap;
import grakn.core.graql.answer.Value;
import grakn.core.graql.concept.AttributeType;
import grakn.core.graql.concept.Thing;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.graph.MovieGraph;
import grakn.core.rule.GraknTestServer;
import grakn.core.server.Session;
import grakn.core.server.Transaction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

import static grakn.core.common.exception.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static grakn.core.graql.query.Graql.count;
import static grakn.core.graql.query.Graql.group;
import static grakn.core.graql.query.Graql.max;
import static grakn.core.graql.query.Graql.mean;
import static grakn.core.graql.query.Graql.median;
import static grakn.core.graql.query.Graql.min;
import static grakn.core.graql.query.Graql.std;
import static grakn.core.graql.query.Graql.sum;
import static grakn.core.graql.query.pattern.Pattern.var;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AggregateQueryIT {

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
        tx = session.transaction(Transaction.Type.WRITE);
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
    public void testCount() {
        List<Value> count = tx.execute(Graql.match(var("x").isa("movie"), var("y").isa("person"), var("r").rel("x").rel("y")).aggregate(count()));

        assertEquals(14, count.get(0).number().intValue());

        count = tx.execute(Graql.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).aggregate(count("x")));

        assertEquals(7, count.get(0).number().intValue());
    }

    @Test
    public void testGroup() {
        AggregateQuery<AnswerGroup<ConceptMap>> groupQuery =
                Graql.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).aggregate(group("x"));
        List<AnswerGroup<ConceptMap>> groups = tx.execute(groupQuery);

        Assert.assertEquals(7, groups.size());

        groups.forEach(group -> {
            group.answers().forEach(conceptMap -> {
                assertEquals(group.owner(), conceptMap.get(var("x")));
                Assert.assertEquals(tx.getEntityType("person"), conceptMap.get(var("y")).asThing().type());
            });
        });
    }

    @Test
    public void testGroupCount() {
        List<AnswerGroup<Value>> groupCount = tx.execute(Graql.match(var("x").isa("movie"), var("r").rel("x"))
                .aggregate(group("x", count())));
        Thing godfather = tx.getAttributeType("title").attribute("Godfather").owner();

        groupCount.forEach(group -> {
            if (group.owner().equals(godfather)) {
                assertEquals(9, group.answers().get(0).number().intValue());
            }
        });
    }

    @Test
    public void testGroupCount2Vars() {
        List<AnswerGroup<Value>> groupCounts = tx.execute(Graql.match(
                var("x").isa("movie"),
                var("y").isa("person"),
                var("z").isa("person"),
                var().rel("x").rel("y")
        ).aggregate(group("x", count("y", "z"))));

        Thing chineseCoffee = tx.getAttributeType("title").attribute("Chinese Coffee").owner();
        Thing godfather = tx.getAttributeType("title").attribute("Godfather").owner();

        boolean containsChineseCoffee = false, containsGodfather = false;

        for (AnswerGroup<Value> group : groupCounts) {
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
        List<AnswerGroup<Value>> groupCount =
                tx.execute(Graql.match(
                        var("x").isa("person"),
                        var("y").isa("movie").has("tmdb-vote-count", var("z")),
                        var().rel("x").rel("y")
                ).aggregate(group("x", max("z"))));

        Thing marlonBrando = tx.getAttributeType("name").attribute("Marlon Brando").owner();
        Thing alPacino = tx.getAttributeType("name").attribute("Al Pacino").owner();

        boolean containsMarlonBrando = false, containsAlPacino = false;

        for (AnswerGroup<Value> group : groupCount) {
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
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(sum("y"));

        assertEquals(1940, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testSumDouble() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(sum("y"));

        assertEquals(27.7d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testSumNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(sum("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMaxInt() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(max("y"));

        assertEquals(1000, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMaxDouble() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(max("y"));

        assertEquals(8.6d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMaxNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(max("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMinInt() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(min("y"));

        assertEquals(5, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMinNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(min("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMean() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(mean("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals((8.6d + 7.6d + 8.4d + 3.1d) / 4d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMeanNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(mean("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testMedianInt() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(median("y"));

        assertEquals(400, tx.execute(query).get(0).number().intValue());
    }

    @Test
    public void testMedianDouble() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(median("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(8.0d, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testMedianNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(median("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testStdDouble1() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie").has("tmdb-vote-count", var("y")))
                .aggregate(std("y"));

        double mean = (1000d + 100d + 400d + 435d + 5d) / 5d;
        double variance = (pow(1000d - mean, 2d) + pow(100d - mean, 2d)
                + pow(400d - mean, 2d) + pow(435d - mean, 2d) + pow(5d - mean, 2d)) / 4d;
        double expected = sqrt(variance);

        assertEquals(expected, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdDouble2() {
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie").has("tmdb-vote-average", var("y")))
                .aggregate(std("y"));

        double mean = (8.6d + 8.4d + 7.6d + 3.1d) / 4d;
        double variance =
                (pow(8.6d - mean, 2d) + pow(8.4d - mean, 2d) + pow(7.6d - mean, 2d) + pow(3.1d - mean, 2d)) / 3d;
        double expected = sqrt(variance);

        assertEquals(expected, tx.execute(query).get(0).number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdNull() {
        tx.putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = Graql.match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(std("y"));

        assertTrue(tx.execute(query).isEmpty());
    }

    @Test
    public void testEmptyMatchCount() {
        assertEquals(0L, tx.execute(Graql.match(var().isa("runtime")).aggregate(count())).get(0).number().longValue());
        tx.execute(Graql.match(var()).aggregate(count()));
    }

    @Test(expected = Exception.class) // TODO: Would help if the error message is more specific
    public void testVarsNotExist() {
        tx.execute(Graql.match(var("x").isa("movie")).aggregate(min("y")));
        System.out.println(tx.execute(Graql.match(var("x").isa("movie")).aggregate(min("x"))));
    }

    @Test(expected = Exception.class)
    public void testMinOnEntity() {
        tx.execute(Graql.match(var("x")).aggregate(min("x")));
    }

    @Test(expected = Exception.class)
    public void testIncorrectResourceDataType() {
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).aggregate(sum("y")));
    }

    @Test
    public void whenGroupVarIsNotInQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(var("z")));
        tx.execute(Graql.match(var("x").isa("movie").has("title", var("y"))).aggregate(group("z", count())));
    }
}

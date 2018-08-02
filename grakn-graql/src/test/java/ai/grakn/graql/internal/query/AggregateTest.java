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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.answer.ConceptMap;
import ai.grakn.graql.answer.Value;
import ai.grakn.matcher.MovieMatchers;
import ai.grakn.test.kbs.MovieKB;
import ai.grakn.test.rule.SampleKBContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

import static ai.grakn.graql.Graql.count;
import static ai.grakn.graql.Graql.group;
import static ai.grakn.graql.Graql.max;
import static ai.grakn.graql.Graql.mean;
import static ai.grakn.graql.Graql.median;
import static ai.grakn.graql.Graql.min;
import static ai.grakn.graql.Graql.std;
import static ai.grakn.graql.Graql.sum;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.ErrorMessage.VARIABLE_NOT_IN_QUERY;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class AggregateTest {

    @ClassRule
    public static final SampleKBContext rule = MovieKB.context();

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    private QueryBuilder qb;

    @Before
    public void setUp() {
        qb = rule.tx().graql();
    }

    @Test
    public void testCount() {
        Value count = qb.match(var("x").isa("movie")).aggregate(count()).execute();
        Assert.assertEquals(MovieMatchers.movies.size(), count.number().intValue());
    }

    @Test
    public void testGroup() {
        AggregateQuery<Map<Concept, List<ConceptMap>>> groupQuery =
                qb.match(var("x").isa("movie"), var("y").isa("person"), var().rel("x").rel("y")).aggregate(group("x"));

        Map<Concept, List<ConceptMap>> groups = groupQuery.execute();

        Assert.assertEquals(MovieMatchers.movies.size(), groups.size());

        groups.forEach((movie, results) -> {
            results.forEach(result -> {
                assertEquals(movie, result.get(Graql.var("x")));
                Assert.assertEquals(rule.tx().getEntityType("person"), result.get(Graql.var("y")).asThing().type());
            });
        });
    }

    @Test
    public void testGroupCount() {
        Map<Concept, Value> groupCount = qb.match(var("x").isa("movie"), var("r").rel("x"))
                .aggregate(group("x", count()))
                .execute();
        Thing godfather = rule.tx().getAttributeType("title").attribute("Godfather").owner();
        assertEquals(9, groupCount.get(godfather).number().intValue());
    }

    @Test
    public void testSumInt() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(sum("y"));

        assertEquals(1940, query.execute().number().intValue());
    }

    @Test
    public void testSumDouble() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(sum("y"));

        assertEquals(27.7d, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testSumNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(sum("y"));

        assertNull(query.execute());
    }

    @Test
    public void testMaxInt() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(max("y"));

        assertEquals(1000, query.execute().number().intValue());
    }

    @Test
    public void testMaxDouble() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(max("y"));

        assertEquals(8.6d, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testMaxNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(max("y"));

        assertNull(query.execute());
    }

    @Test
    public void testMinInt() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(min("y"));

        assertEquals(5, query.execute().number().intValue());
    }

    @Test
    public void testMinNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(min("y"));

        assertNull(query.execute());
    }

    @Test
    public void testMean() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(mean("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals((8.6d + 7.6d + 8.4d + 3.1d) / 4d, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testMeanNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(mean("y"));

        assertNull(query.execute());
    }

    @Test
    public void testMedianInt() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-count"))
                .aggregate(median("y"));

        assertEquals(400, query.execute().number().intValue());
    }

    @Test
    public void testMedianDouble() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("tmdb-vote-average"))
                .aggregate(median("y"));

        //noinspection OptionalGetWithoutIsPresent
        assertEquals(8.0d, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testMedianNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(median("y"));

        assertNull(query.execute());
    }

    @Test
    public void testStdDouble1() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie").has("tmdb-vote-count", var("y")))
                .aggregate(std("y"));

        double mean = (1000d + 100d + 400d + 435d + 5d) / 5d;
        double variance = (pow(1000d - mean, 2d) + pow(100d - mean, 2d)
                + pow(400d - mean, 2d) + pow(435d - mean, 2d) + pow(5d - mean, 2d)) / 4d;
        double expected = sqrt(variance);

        assertEquals(expected, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdDouble2() {
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie").has("tmdb-vote-average", var("y")))
                .aggregate(std("y"));

        double mean = (8.6d + 8.4d + 7.6d + 3.1d) / 4d;
        double variance =
                (pow(8.6d - mean, 2d) + pow(8.4d - mean, 2d) + pow(7.6d - mean, 2d) + pow(3.1d - mean, 2d)) / 3d;
        double expected = sqrt(variance);

        assertEquals(expected, query.execute().number().doubleValue(), 0.01d);
    }

    @Test
    public void testStdNull() {
        rule.tx().putAttributeType("random", AttributeType.DataType.INTEGER);
        AggregateQuery<Value> query = qb
                .match(var("x").isa("movie"), var().rel("x").rel("y"), var("y").isa("random"))
                .aggregate(std("y"));

        assertNull(query.execute());
    }

    @Test
    public void testEmptyMatchCount() {
        assertEquals(0L, rule.tx().graql().match(var().isa("runtime")).aggregate(count()).execute().number().longValue());
        rule.tx().graql().match(var()).aggregate(count()).execute();
    }

    @Test(expected = Exception.class) // TODO: Would help if the error message is more specific
    public void testVarsNotExist() {
        rule.tx().graql().match(var("x").isa("movie")).aggregate(min("y")).execute();
        System.out.println(rule.tx().graql().match(var("x").isa("movie")).aggregate(min("x")).execute());
    }

    @Test(expected = Exception.class)
    public void testMinOnEntity() {
        rule.tx().graql().match(var("x")).aggregate(min("x")).execute();
    }

    @Test(expected = Exception.class)
    public void testIncorrectResourceDataType() {
        rule.tx().graql().match(var("x").isa("movie").has("title", var("y")))
                .aggregate(sum("y")).execute();
    }

    @Test
    public void whenGroupVarIsNotInQuery_Throw() {
        exception.expect(GraqlQueryException.class);
        exception.expectMessage(VARIABLE_NOT_IN_QUERY.getMessage(Graql.var("z")));
        rule.tx().graql().match(var("x").isa("movie").has("title", var("y"))).aggregate(group("z", count())).execute();
    }
}

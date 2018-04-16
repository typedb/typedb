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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query;

import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.admin.MatchAdmin;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AggregateQueryImplTest {

    private final MatchAdmin match1 = Graql.match(var("x").isa("movie")).admin();
    private final MatchAdmin match2 = Graql.match(var("y").isa("movie")).admin();

    private final Aggregate<Object, Long> aggregate1 = Aggregates.count();
    private final Aggregate<Answer, Number> aggregate2 = Aggregates.sum(Graql.var("x"));

    @Test
    public void aggregateQueriesWithTheSameMatchAndAggregatesAreEqual() {
        AggregateQuery<?> query1 = Queries.aggregate(match1, aggregate1);
        AggregateQuery<?> query2 = Queries.aggregate(match1, aggregate1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void aggregateQueriesWithDifferentMatchesAreDifferent() {
        AggregateQuery<?> query1 = Queries.aggregate(match1, aggregate1);
        AggregateQuery<?> query2 = Queries.aggregate(match2, aggregate1);

        assertNotEquals(query1, query2);
    }

    @Test
    public void aggregateQueriesWithDifferentDeletersAreDifferent() {
        AggregateQuery<?> query1 = Queries.aggregate(match1, aggregate1);
        AggregateQuery<?> query2 = Queries.aggregate(match1, aggregate2);

        assertNotEquals(query1, query2);
    }
}
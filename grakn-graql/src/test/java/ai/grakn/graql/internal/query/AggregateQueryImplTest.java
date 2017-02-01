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

package ai.grakn.graql.internal.query;

import ai.grakn.concept.Concept;
import ai.grakn.graql.Aggregate;
import ai.grakn.graql.AggregateQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.internal.query.aggregate.Aggregates;
import org.junit.Test;

import java.util.Map;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AggregateQueryImplTest {

    private final MatchQueryAdmin match1 = Graql.match(var("x").isa("movie")).admin();
    private final MatchQueryAdmin match2 = Graql.match(var("y").isa("movie")).admin();

    private final Aggregate<Object, Long> aggregate1 = Aggregates.count();
    private final Aggregate<Map<VarName, Concept>, Number> aggregate2 = Aggregates.sum(VarName.of("x"));

    @Test
    public void aggregateQueriesWithTheSameMatchQueryAndAggregatesAreEqual() {
        AggregateQuery<?> query1 = new AggregateQueryImpl<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQueryImpl<>(match1, aggregate1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void aggregateQueriesWithDifferentMatchQueriesAreDifferent() {
        AggregateQuery<?> query1 = new AggregateQueryImpl<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQueryImpl<>(match2, aggregate1);

        assertNotEquals(query1, query2);
    }

    @Test
    public void aggregateQueriesWithDifferentDeletersAreDifferent() {
        AggregateQuery<?> query1 = new AggregateQueryImpl<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQueryImpl<>(match1, aggregate2);

        assertNotEquals(query1, query2);
    }
}
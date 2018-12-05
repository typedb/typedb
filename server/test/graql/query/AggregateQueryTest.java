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

import grakn.core.graql.admin.MatchAdmin;
import grakn.core.graql.answer.Value;
import grakn.core.graql.query.pattern.Pattern;
import org.junit.Test;

import static grakn.core.graql.query.pattern.Pattern.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AggregateQueryTest {

    private final MatchAdmin match1 = Graql.match(var("x").isa("movie")).admin();
    private final MatchAdmin match2 = Graql.match(var("y").isa("movie")).admin();

    private final Aggregate<Value> aggregate1 = Graql.count();
    private final Aggregate<Value> aggregate2 = Graql.sum(Pattern.var("x"));

    @Test
    public void aggregateQueriesWithTheSameMatchAndAggregatesAreEqual() {
        AggregateQuery<?> query1 = new AggregateQuery<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQuery<>(match1, aggregate1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void aggregateQueriesWithDifferentMatchesAreDifferent() {
        AggregateQuery<?> query1 = new AggregateQuery<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQuery<>(match2, aggregate1);

        assertNotEquals(query1, query2);
    }

    @Test
    public void aggregateQueriesWithDifferentDeletersAreDifferent() {
        AggregateQuery<?> query1 = new AggregateQuery<>(match1, aggregate1);
        AggregateQuery<?> query2 = new AggregateQuery<>(match1, aggregate2);

        assertNotEquals(query1, query2);
    }
}
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

import grakn.core.graql.query.pattern.VarPatternAdmin;
import grakn.core.server.Transaction;
import grakn.core.graql.admin.MatchAdmin;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class InsertQueryTest {

    private final MatchAdmin match1 = Graql.match(var("x").isa("movie")).admin();
    private final MatchAdmin match2 = Graql.match(var("y").isa("movie")).admin();

    private final ImmutableCollection<VarPatternAdmin> vars1 = ImmutableSet.of(var("x").admin());
    private final ImmutableCollection<VarPatternAdmin> vars2 = ImmutableSet.of(var("y").admin());

    @Test
    public void insertQueriesWithTheSameVarsAndQueryAreEqual() {
        InsertQuery query1 = InsertQuery.create(null, match1, vars1);
        InsertQuery query2 = InsertQuery.create(null, match1, vars1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithTheSameVarsAndGraphAreEqual() {
        Transaction graph = mock(Transaction.class);

        InsertQuery query1 = InsertQuery.create(graph, null, vars1);
        InsertQuery query2 = InsertQuery.create(graph, null, vars1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithDifferentMatchesAreDifferent() {
        InsertQuery query1 = InsertQuery.create(null, match1, vars1);
        InsertQuery query2 = InsertQuery.create(null, match2, vars1);

        assertNotEquals(query1, query2);
    }

    @Test
    public void insertQueriesWithDifferentGraphsAreDifferent() {
        Transaction graph1 = mock(Transaction.class);
        Transaction graph2 = mock(Transaction.class);

        InsertQuery query1 = InsertQuery.create(graph1, null, vars1);
        InsertQuery query2 = InsertQuery.create(graph2, null, vars2);

        assertNotEquals(query1, query2);
    }

    @Test
    public void insertQueriesWithDifferentVarsAreDifferent() {
        InsertQuery query1 = InsertQuery.create(null, match1, vars1);
        InsertQuery query2 = InsertQuery.create(null, match1, vars2);

        assertNotEquals(query1, query2);
    }
}
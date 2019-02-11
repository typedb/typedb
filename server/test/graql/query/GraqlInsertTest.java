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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import grakn.core.graql.query.query.GraqlInsert;
import grakn.core.graql.query.query.MatchClause;
import grakn.core.graql.query.statement.Statement;
import grakn.core.server.Transaction;
import org.junit.Test;

import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class GraqlInsertTest {

    private final MatchClause match1 = Graql.match(var("x").isa("movie"));
    private final MatchClause match2 = Graql.match(var("y").isa("movie"));

    private final ImmutableCollection<Statement> vars1 = ImmutableSet.of(var("x"));
    private final ImmutableCollection<Statement> vars2 = ImmutableSet.of(var("y"));

    @Test
    public void insertQueriesWithTheSameVarsAndQueryAreEqual() {
        GraqlInsert query1 = new GraqlInsert(match1, ImmutableList.copyOf(vars1));
        GraqlInsert query2 = new GraqlInsert(match1, ImmutableList.copyOf(vars1));

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithTheSameVarsAndGraphAreEqual() {
        Transaction graph = mock(Transaction.class);

        GraqlInsert query1 = new GraqlInsert(null, ImmutableList.copyOf(vars1));
        GraqlInsert query2 = new GraqlInsert(null, ImmutableList.copyOf(vars1));

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithDifferentMatchesAreDifferent() {
        GraqlInsert query1 = new GraqlInsert(match1, ImmutableList.copyOf(vars1));
        GraqlInsert query2 = new GraqlInsert(match2, ImmutableList.copyOf(vars1));

        assertNotEquals(query1, query2);
    }

    @Test
    public void insertQueriesWithDifferentVarsAreDifferent() {
        GraqlInsert query1 = new GraqlInsert(match1, ImmutableList.copyOf(vars1));
        GraqlInsert query2 = new GraqlInsert(match1, ImmutableList.copyOf(vars2));

        assertNotEquals(query1, query2);
    }
}
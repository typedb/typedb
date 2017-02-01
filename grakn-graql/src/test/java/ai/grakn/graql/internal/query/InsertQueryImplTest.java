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

import ai.grakn.GraknGraph;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.admin.MatchQueryAdmin;
import ai.grakn.graql.admin.VarAdmin;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class InsertQueryImplTest {

    private final Optional<MatchQueryAdmin> match1 = Optional.of(Graql.match(var("x").isa("movie")).admin());
    private final Optional<MatchQueryAdmin> match2 = Optional.of(Graql.match(var("y").isa("movie")).admin());

    private final ImmutableCollection<VarAdmin> vars1 = ImmutableSet.of(var("x").admin());
    private final ImmutableCollection<VarAdmin> vars2 = ImmutableSet.of(var("y").admin());

    @Test
    public void insertQueriesWithTheSameVarsAndMatchQueryAreEqual() {
        InsertQuery query1 = new InsertQueryImpl(vars1, match1, Optional.empty());
        InsertQuery query2 = new InsertQueryImpl(vars1, match1, Optional.empty());

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithTheSameVarsAndGraphAreEqual() {
        GraknGraph graph = mock(GraknGraph.class);

        InsertQuery query1 = new InsertQueryImpl(vars1, Optional.empty(), Optional.of(graph));
        InsertQuery query2 = new InsertQueryImpl(vars1, Optional.empty(), Optional.of(graph));

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void insertQueriesWithDifferentMatchQueriesAreDifferent() {
        InsertQuery query1 = new InsertQueryImpl(vars1, match1, Optional.empty());
        InsertQuery query2 = new InsertQueryImpl(vars1, match2, Optional.empty());

        assertNotEquals(query1, query2);
    }

    @Test
    public void insertQueriesWithDifferentGraphsAreDifferent() {
        GraknGraph graph1 = mock(GraknGraph.class);
        GraknGraph graph2 = mock(GraknGraph.class);

        InsertQuery query1 = new InsertQueryImpl(vars1, Optional.empty(), Optional.of(graph1));
        InsertQuery query2 = new InsertQueryImpl(vars2, Optional.empty(), Optional.of(graph2));

        assertNotEquals(query1, query2);
    }

    @Test
    public void insertQueriesWithDifferentVarsAreDifferent() {
        InsertQuery query1 = new InsertQueryImpl(vars1, match1, Optional.empty());
        InsertQuery query2 = new InsertQueryImpl(vars2, match1, Optional.empty());

        assertNotEquals(query1, query2);
    }
}
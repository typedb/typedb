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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.query.match;

import ai.grakn.GraknTx;
import ai.grakn.graql.internal.pattern.Patterns;
import com.google.common.collect.Sets;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

public class MatchKBTest {

    private final AbstractMatch query =
            new MatchBase(Patterns.conjunction(Sets.newHashSet(var("x").admin())));

    @Test
    public void matchesContainingTheSameGraphAndMatchBaseAreEqual() {
        GraknTx graph = mock(GraknTx.class);

        MatchTx query1 = new MatchTx(graph, query);
        MatchTx query2 = new MatchTx(graph, query);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void matchesContainingDifferentGraphsAreNotEqual() {
        GraknTx graph1 = mock(GraknTx.class);
        GraknTx graph2 = mock(GraknTx.class);

        MatchTx query1 = new MatchTx(graph1, query);
        MatchTx query2 = new MatchTx(graph2, query);

        assertNotEquals(query1, query2);
    }
}
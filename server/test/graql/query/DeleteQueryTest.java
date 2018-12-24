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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.graql.query.pattern.Variable;
import org.junit.Test;

import java.util.Collection;

import static grakn.core.graql.query.pattern.Pattern.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DeleteQueryTest {

    private final MatchClause match1 = Graql.match(var("x").isa("movie"));
    private final MatchClause match2 = Graql.match(var("y").isa("movie"));

    private final Collection<Variable> vars1 = Sets.newHashSet(new Variable("x"));
    private final Collection<Variable> vars2 = Sets.newHashSet(new Variable("y"));

    @Test
    public void deleteQueriesWithTheSameMatchAndVarsAreEqual() {
        DeleteQuery query1 = new DeleteQuery(match1, ImmutableSet.copyOf(vars1));
        DeleteQuery query2 = new DeleteQuery(match1, ImmutableSet.copyOf(vars1));

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void deleteQueriesWithDifferentMatchesOrVarsAreDifferent() {
        DeleteQuery query1 = new DeleteQuery(match1, ImmutableSet.copyOf(vars1));
        DeleteQuery query2 = new DeleteQuery(match2, ImmutableSet.copyOf(vars2));

        assertNotEquals(query1, query2);
    }
}
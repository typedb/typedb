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

import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.admin.VarAdmin;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DeleteQueryImplTest {

    private final MatchQuery match1 = Graql.match(var("x").isa("movie"));
    private final MatchQuery match2 = Graql.match(var("y").isa("movie"));

    private final Collection<VarAdmin> deleters1 = Sets.newHashSet(var("x").admin());
    private final Collection<VarAdmin> deleters2 = Sets.newHashSet(var("y").admin());

    @Test
    public void deleteQueriesWithTheSameMatchQueryAndDeletersAreEqual() {
        DeleteQuery query1 = new DeleteQueryImpl(deleters1, match1);
        DeleteQuery query2 = new DeleteQueryImpl(deleters1, match1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void deleteQueriesWithDifferentMatchQueriesAreDifferent() {
        DeleteQuery query1 = new DeleteQueryImpl(deleters1, match1);
        DeleteQuery query2 = new DeleteQueryImpl(deleters1, match2);

        assertNotEquals(query1, query2);
    }

    @Test
    public void deleteQueriesWithDifferentDeletersAreDifferent() {
        DeleteQuery query1 = new DeleteQueryImpl(deleters1, match1);
        DeleteQuery query2 = new DeleteQueryImpl(deleters2, match1);

        assertNotEquals(query1, query2);
    }
}
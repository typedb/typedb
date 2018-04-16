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
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.query;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.graql.DeleteQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Match;
import ai.grakn.graql.Var;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Collection;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DeleteQueryImplTest {

    private final Match match1 = Graql.match(var("x").isa("movie"));
    private final Match match2 = Graql.match(var("y").isa("movie"));

    private final Collection<Var> vars1 = Sets.newHashSet(var("x"));
    private final Collection<Var> vars2 = Sets.newHashSet(var("y"));

    @Test
    public void deleteQueriesWithTheSameMatchAndVarsAreEqual() {
        DeleteQuery query1 = DeleteQueryImpl.of(vars1, match1);
        DeleteQuery query2 = DeleteQueryImpl.of(vars1, match1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void deleteQueriesWithDifferentMatchesAreDifferent() {
        DeleteQuery query1 = DeleteQueryImpl.of(vars1, match1);
        DeleteQuery query2 = DeleteQueryImpl.of(vars1, match2);

        assertNotEquals(query1, query2);
    }

    @Test
    public void deleteQueriesWithDifferentVarsAreDifferent() {
        DeleteQuery query1 = DeleteQueryImpl.of(vars1, match1);
        DeleteQuery query2 = DeleteQueryImpl.of(vars2, match1);

        assertNotEquals(query1, query2);
    }
}

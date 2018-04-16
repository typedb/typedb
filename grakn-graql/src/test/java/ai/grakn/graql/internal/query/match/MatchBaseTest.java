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

package ai.grakn.graql.internal.query.match;

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

import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.internal.pattern.Patterns;
import com.google.common.collect.Sets;
import org.junit.Test;

import static ai.grakn.graql.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MatchBaseTest {

    private final Conjunction<PatternAdmin> pattern1 = Patterns.conjunction(Sets.newHashSet(
            var("x").isa("movie").admin(),
            var().rel("x").rel("y").admin()
    ));

    private final Conjunction<PatternAdmin> pattern2 = Patterns.conjunction(Sets.newHashSet(
            var("x").isa("movie").has("title", var("y")).admin()
    ));

    @Test
    public void matchesContainingTheSamePatternAreEqual() {
        MatchBase query1 = new MatchBase(pattern1);
        MatchBase query2 = new MatchBase(pattern1);

        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
    }

    @Test
    public void matchesContainingDifferentPatternsAreNotEqual() {
        MatchBase query1 = new MatchBase(pattern1);
        MatchBase query2 = new MatchBase(pattern2);

        assertNotEquals(query1, query2);
    }
}

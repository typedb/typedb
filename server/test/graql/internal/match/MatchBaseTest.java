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

package grakn.core.graql.internal.match;

import grakn.core.graql.query.Graql;
import grakn.core.graql.query.pattern.Conjunction;
import grakn.core.graql.query.pattern.Pattern;
import com.google.common.collect.Sets;
import org.junit.Test;

import static grakn.core.graql.query.Graql.var;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MatchBaseTest {

    private final Conjunction<Pattern> pattern1 = Graql.and(Sets.newHashSet(
            var("x").isa("movie"),
            var().rel("x").rel("y")
    ));

    private final Conjunction<Pattern> pattern2 = Graql.and(Sets.newHashSet(
            (Pattern) var("x").isa("movie").has("title", var("y"))
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
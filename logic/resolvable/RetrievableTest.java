/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.logic.resolvable;

import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import graql.lang.Graql;
import org.junit.Test;

import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class RetrievableTest {

    private Conjunction parseConjunction(String query) {
        return Disjunction.create(Graql.parsePattern(query).asConjunction().normalise()).conjunctions().iterator().next();
    }

    @Test
    public void test_basic() {
        Set<Concludable<?>> concludables = Concludable.create(parseConjunction("{ $p has $n; }"));
        Set<Retrievable> retrievables = Retrievable.extractFrom(parseConjunction("{ $p isa person; $p has $n; $n isa name; }"), concludables);
        assertEquals(1, concludables.size());
        assertEquals(1, retrievables.size());
    }
}

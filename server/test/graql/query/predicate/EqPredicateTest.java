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

package grakn.core.graql.query.predicate;

import grakn.core.graql.query.pattern.Pattern;
import grakn.core.graql.query.pattern.Patterns;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class EqPredicateTest {

    @Test
    public void predicatesEqualEvenWhenContainingIntegersAndLongs() {
        int intNum = 20;
        long longNum = 20L;

        EqPredicate intPredicate = new EqPredicate(intNum);
        EqPredicate longPredicate = new EqPredicate(longNum);

        assertEquals(intPredicate, longPredicate);
        assertEquals(intPredicate.hashCode(), longPredicate.hashCode());
    }

    @Test
    public void whenAnEqPredicateContainsAVariable_ItIsNotSpecific() {
        EqPredicate predicate = new EqPredicate(Patterns.var("x"));
        assertFalse(predicate.isSpecific());
    }
}
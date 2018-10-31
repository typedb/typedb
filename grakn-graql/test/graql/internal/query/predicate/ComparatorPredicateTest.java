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

package ai.grakn.graql.internal.query.predicate;

import ai.grakn.exception.GraqlQueryException;
import com.google.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.LocalDate;

import static ai.grakn.util.ErrorMessage.INVALID_VALUE;

public class ComparatorPredicateTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void whenPassedAString_DontThrow() {
        new MyComparatorPredicate("hello");
    }

    @Test
    public void whenPassedAnInt_DontThrow() {
        new MyComparatorPredicate(1);
    }

    @Test
    public void whenPassedALong_DontThrow() {
        new MyComparatorPredicate(1L);
    }

    @Test
    public void whenPassedADouble_DontThrow() {
        new MyComparatorPredicate(1d);
    }

    @Test
    public void whenPassedABoolean_DontThrow() {
        new MyComparatorPredicate(false);
    }

    @Test
    public void whenPassedALocalDateTime_DontThrow() {
        new MyComparatorPredicate(LocalDate.of(2012, 12, 21).atStartOfDay());
    }

    @Test(expected = NullPointerException.class)
    public void whenPassedANull_Throw() {
        new MyComparatorPredicate(null);
    }

    @Test
    public void whenPassedAnUnsupportedType_Throw() {
        ImmutableList<Integer> value = ImmutableList.of(1, 2, 3);

        exception.expect(GraqlQueryException.class);
        exception.expectMessage(INVALID_VALUE.getMessage(value.getClass()));
        new MyComparatorPredicate(value).persistedValue();
    }
}

class MyComparatorPredicate extends ComparatorPredicate {

    MyComparatorPredicate(Object value) {
        super(value);
    }

    @Override
    protected String getSymbol() {
        return "@";
    }

    @Override
    <V> P<V> gremlinPredicate(V value) {
        return P.eq(value);
    }

    @Override
    public int signum() { return 0; }
}
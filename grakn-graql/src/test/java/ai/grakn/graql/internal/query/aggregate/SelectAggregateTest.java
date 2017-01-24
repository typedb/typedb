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

package ai.grakn.graql.internal.query.aggregate;

import ai.grakn.graql.NamedAggregate;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static ai.grakn.graql.internal.query.aggregate.Aggregates.count;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SelectAggregateTest {

    private ImmutableSet<NamedAggregate<? super Object, ? extends Long>> set1 =
            ImmutableSet.of(count().as("a"));

    private ImmutableSet<NamedAggregate<? super Object, ? extends Long>> set2 =
            ImmutableSet.of(count().as("l"), count().as("c"));

    @Test
    public void selectAggregatesContainingTheSamePropertiesAreEqual() {
        SelectAggregate<Object, Long> aggregate1 = new SelectAggregate<>(set1);
        SelectAggregate<Object, Long> aggregate2 = new SelectAggregate<>(set1);

        assertEquals(aggregate1, aggregate2);
        assertEquals(aggregate1.hashCode(), aggregate2.hashCode());
    }

    @Test
    public void selectAggregatesContainingDifferentPropertiesAreDifferent() {
        SelectAggregate<Object, Long> aggregate1 = new SelectAggregate<>(set1);
        SelectAggregate<Object, Long> aggregate2 = new SelectAggregate<>(set2);

        assertNotEquals(aggregate1, aggregate2);
    }
}
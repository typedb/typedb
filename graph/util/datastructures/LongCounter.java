/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
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

package grakn.core.graph.util.datastructures;

import java.io.Serializable;

/**
 * A counter with a long value
 *
 */
public class LongCounter implements Serializable {

    private static final long serialVersionUID = -880751358315110930L;


    private long count;

    public LongCounter(long initial) {
        count = initial;
    }

    public LongCounter() {
        this(0);
    }

    public void increment(long delta) {
        count += delta;
    }

    public void decrement(long delta) {
        count -= delta;
    }

    public void set(long value) {
        count = value;
    }

    public long get() {
        return count;
    }

    @Override
    public boolean equals(Object other) {
        return this == other;
    }

}

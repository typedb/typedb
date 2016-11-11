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
 */

package ai.grakn.graql.internal.gremlin;

import com.google.common.collect.ImmutableSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;

import java.util.stream.Stream;

/**
 * a pattern to match in the graph. comprised of {@code Fragments}, each describing one way to represent the traversal,
 * starting from different variables.
 * <p>
 * A {@code EquivalentFragmentSet} may contain only one {@code Fragment} (e.g. checking the 'id' property), while others may
 * be comprised of two fragments (e.g. $x isa $y, which may start from $x or $y).
 */
public class EquivalentFragmentSet {

    private final ImmutableSet<Fragment> fragments;

    public static EquivalentFragmentSet create(Fragment... fragments) {
        return new EquivalentFragmentSet(fragments);
    }

    /**
     * @param fragments an array of Fragments that this EquivalentFragmentSet contains
     */
    private EquivalentFragmentSet(Fragment... fragments) {
        this.fragments = ImmutableSet.copyOf(fragments);
        this.fragments.forEach(f -> f.setEquivalentFragmentSet(this));
    }

    /**
     * @return a stream of fragments that this EquivalentFragmentSet contains
     */
    Stream<Fragment> getFragments() {
        return fragments.stream();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EquivalentFragmentSet that = (EquivalentFragmentSet) o;

        return fragments != null ? fragments.equals(that.fragments) : that.fragments == null;

    }

    @Override
    public int hashCode() {
        return fragments != null ? fragments.hashCode() : 0;
    }
}

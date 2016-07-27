/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.gremlin;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

/**
 * a pattern to match in the graph. comprised of {@code Fragments}, each describing one way to represent the traversal,
 * starting from different variables.
 * <p>
 * A {@code MultiTraversal} may contain only one {@code Fragment} (e.g. checking the 'id' property), while others may
 * be comprised of two fragments (e.g. $x isa $y, which may start from $x or $y).
 */
public class MultiTraversal {

    private final Collection<Fragment> fragments;

    /**
     * @param fragments an array of Fragments that this MultiTraversal contains
     */
    public MultiTraversal(Fragment... fragments) {
        this.fragments = Arrays.asList(fragments);
        this.fragments.forEach(f -> f.setMultiTraversal(this));
    }

    /**
     * @return a stream of fragments that this MultiTraversal contains
     */
    public Stream<Fragment> getFragments() {
        return fragments.stream();
    }
}

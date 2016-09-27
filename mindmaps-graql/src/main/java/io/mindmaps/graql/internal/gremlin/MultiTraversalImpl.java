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

public class MultiTraversalImpl implements MultiTraversal {

    private final Collection<Fragment> fragments;

    /**
     * @param fragments an array of Fragments that this MultiTraversal contains
     */
    public MultiTraversalImpl(Fragment... fragments) {
        this.fragments = Arrays.asList(fragments);
        this.fragments.forEach(f -> f.setMultiTraversal(this));
    }

    @Override
    public Stream<Fragment> getFragments() {
        return fragments.stream();
    }
}

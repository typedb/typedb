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

package io.mindmaps.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.mindmaps.graql.internal.gremlin.*;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;

interface SingleTraversalProperty extends VarPropertyInternal {

    GraphTraversal<Vertex, Vertex> applyTraversal(GraphTraversal<Vertex, Vertex> traversal);

    FragmentPriority getPriority();

    @Override
    default Collection<MultiTraversal> match(String start) {
        Fragment fragment = Fragment.create(this::applyTraversal, getPriority(), start);
        MultiTraversal multiTraversal = MultiTraversal.create(fragment);
        return Sets.newHashSet(multiTraversal);
    }
}

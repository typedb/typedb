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

package io.mindmaps.core.implementation;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * A mindmaps graph which uses a Tinkergraph backend.
 * Primarily used for testing
 */
public class MindmapsTinkerGraph extends AbstractMindmapsGraph<TinkerGraph> {
    public MindmapsTinkerGraph(TinkerGraph tinkerGraph, String name, boolean batchLoading){
        super(tinkerGraph, name, "localhost", batchLoading);
    }

    /**
     * Clears the graph completely.
     */
    @Override
    public void clear() {
        getTinkerPopGraph().traversal().V().drop().iterate();
    }
}

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

package io.mindmaps.factory;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.core.implementation.MindmapsTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.UUID;

/**
 * An in memory test graph. Should only be used for testing purposes.
 */
public class MindmapsTestGraphFactory {
    private MindmapsTestGraphFactory(){
        throw new UnsupportedOperationException();
    }

    /**
     *
     * @return An empty mindmaps graph with a tinker graph backend
     */
    public static MindmapsGraph newEmptyGraph(){
        return new MindmapsTinkerGraph(TinkerGraph.open(), UUID.randomUUID().toString(), false);
    }

    /**
     *
     * @return An empty mindmaps graph with a tinker graph backend which has batch loading enabled
     */
    public static MindmapsGraph newBatchLoadingEmptyGraph(){
        return new MindmapsTinkerGraph(TinkerGraph.open(), UUID.randomUUID().toString(), true);
    }
}

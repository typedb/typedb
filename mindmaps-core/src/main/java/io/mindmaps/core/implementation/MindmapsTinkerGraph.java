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

import io.mindmaps.core.MindmapsTransaction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

/**
 * A mindmaps graph which produces new transactions to work with using a Tinkergraph backend.
 * Primarily used for testing
 */
public class MindmapsTinkerGraph extends AbstractMindmapsGraph<TinkerGraph> {
    public MindmapsTinkerGraph(TinkerGraph tinkerGraph, String name){
        super(tinkerGraph, name, "localhost");
        new MindmapsTinkerTransaction(this).initialiseMetaConcepts();
    }

    /**
     * Clears the graph completely. WARNING: This will invalidate any open transactions.
     */
    @Override
    public void clear() {
        close();
    }

    @Override
    protected MindmapsTransaction buildTransaction() {
        return new MindmapsTinkerTransaction(this);
    }
}

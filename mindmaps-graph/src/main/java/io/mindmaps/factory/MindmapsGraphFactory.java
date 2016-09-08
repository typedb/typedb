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
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * The interface used to build new graphs from different vendors.
 * Adding new vendor support means implementing this interface.
 */
interface MindmapsGraphFactory<M extends MindmapsGraph, T extends Graph> {
    /**
     *
     * @param name The name of the graph we should be initialising
     * @param address The address of where the backend is. Defaults to localhost if null
     * @param pathToConfig Path to file storing optional configuration parameters. Uses defaults if left null
     * @param batchLoading A flag which indicates if the graph has batch loading enabled or not.
     * @return An instance of Mindmaps graph
     */
    M getGraph(String name, String address, String pathToConfig, boolean batchLoading);

    /**
     *
     * @param name The name of the graph we should be initialising
     * @param address The address of where the backend is. Defaults to localhost if null
     * @param pathToConfig Path to file storing optional configuration parameters. Uses defaults if left null
     * @param batchLoading A flag which indicates if the graph has batch loading enabled or not.
     * @return An instance of a tinker graph
     */
    T getTinkerPopGraph(String name, String address, String pathToConfig, boolean batchLoading);
}

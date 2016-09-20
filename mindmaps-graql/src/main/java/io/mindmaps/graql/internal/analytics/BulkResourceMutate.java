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

package io.mindmaps.graql.internal.analytics;

import io.mindmaps.MindmapsGraph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Methods to deal with persisting values to a Mindmaps graph during OLAP computations. Each spark executor is thread
 * bound and responsible for a subset of the vertices in the graph. Therefore, an instance of the graph can be held in
 * each executor and the mutations from multiple vertices committed as batches. The need to delete relations in a
 * separate iterations from when new relations are added can also be facilitated.
 *
 * Each vertex program should instatiate the <code>BulkResourceMutate</code> class at the start of an iteration and call
 * the <code>close</code> method at the end of each iteration. Additionally <code>cleanup</code> must be called in a
 * separate iteration from <code>putValue</code> to ensure that the graph remains sound.
 */

public class BulkResourceMutate {

    int batchSize = 100;
    boolean havePutValue = false;
    boolean haveCalledCleanup = false;
    private MindmapsGraph graph;
    private int currentNumberOfVertices = 0;
    private String resourceTypeId;

    void putValue(Vertex vertex) {}

    void cleanup(Vertex vertex) {}

}

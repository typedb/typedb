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

package grakn.core.graph.graphdb.olap;

import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanJob;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;

/**
 * Expresses a computation over all vertices ina a graph database. Process is called for each vertex that is previously
 * loaded from disk. To limit the data that needs to be pulled out of the database, the query can specify the queries
 * (which need to be vertex-centric) for the data that is needed. Only this data is then pulled. If the user attempts
 * to access additional data during processing, the behavior is undefined.
 */
public interface VertexScanJob extends Cloneable {

    /**
     * @see ScanJob
     */
    default void workerIterationStart(JanusGraph graph, Configuration config, ScanMetrics metrics) {
    }

    /**
     * @see ScanJob
     */
    default void workerIterationEnd(ScanMetrics metrics) {
    }

    /**
     * Process the given vertex with its adjacency list and properties pre-loaded.
     */
    void process(JanusGraphVertex vertex, ScanMetrics metrics);


    /**
     * Specify the queries for the data to be loaded into the vertices prior to processing.
     */
    void getQueries(QueryContainer queries);

    /**
     * Returns a clone of this VertexScanJob. The clone will not yet be initialized for computation but all of
     * its internal state (if any) must match that of the original copy.
     *
     * @return A clone of this job
     */
    VertexScanJob clone();

}

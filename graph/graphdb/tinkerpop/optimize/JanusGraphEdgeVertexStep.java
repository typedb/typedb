/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.graph.graphdb.tinkerpop.optimize;

import com.google.common.collect.Sets;
import grakn.core.graph.core.JanusGraphMultiVertexQuery;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import grakn.core.graph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A JanusGraphEdgeVertexStep is identical to a EdgeVertexStep. The only difference
 * being that it can use multiQuery to pre-fetch the vertex properties prior to the execution
 * of any subsequent has steps and so eliminate the need for a network trip for each vertex.
 * It implements the optimisation enabled via the query.batch-property-prefetch config option.
 */
public class JanusGraphEdgeVertexStep extends EdgeVertexStep implements Profiling {

    private boolean initialized = false;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private final int txVertexCacheSize;

    public JanusGraphEdgeVertexStep(EdgeVertexStep originalStep, int txVertexCacheSize) {
        super(originalStep.getTraversal(), originalStep.getDirection());
        originalStep.getLabels().forEach(this::addLabel);
        this.txVertexCacheSize = txVertexCacheSize;
    }

    private void initialize() {
        initialized = true;

        if (!starts.hasNext()) {
            throw FastNoSuchElementException.instance();
        }
        List<Traverser.Admin<Edge>> edges = new ArrayList<>();
        Set<Vertex> vertices = Sets.newHashSet();
        starts.forEachRemaining(e -> {
            edges.add(e);

            if (vertices.size() < txVertexCacheSize) {
                if (Direction.IN.equals(direction) || Direction.BOTH.equals(direction)) {
                    vertices.add(e.get().inVertex());
                }
                if (Direction.OUT.equals(direction) || Direction.BOTH.equals(direction)) {
                    vertices.add(e.get().outVertex());
                }
            }
        });

        // If there are multiple vertices then fetch the properties for all of them in a single multiQuery to
        // populate the vertex cache so subsequent queries of properties don't have to go to the storage back end
        if (vertices.size() > 1) {
            JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
            ((BasicVertexCentricQueryBuilder) multiQuery).profiler(queryProfiler);
            multiQuery.addAllVertices(vertices).preFetch();
        }

        starts.add(edges.iterator());
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public JanusGraphEdgeVertexStep clone() {
        final JanusGraphEdgeVertexStep clone = (JanusGraphEdgeVertexStep) super.clone();
        clone.initialized = false;
        return clone;
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }
}

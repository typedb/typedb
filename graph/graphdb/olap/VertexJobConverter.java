// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grakn.core.graph.graphdb.olap;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphFactory;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.BasicConfiguration;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanJob;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.graphdb.database.StandardJanusGraph;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.olap.QueryContainer;
import grakn.core.graph.graphdb.olap.VertexScanJob;
import grakn.core.graph.graphdb.relations.RelationCache;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.transaction.StandardTransactionBuilder;
import grakn.core.graph.graphdb.types.system.BaseKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;


public class VertexJobConverter implements ScanJob {

    protected static final SliceQuery VERTEX_EXISTS_QUERY = new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(4)).setLimit(1);

    public static final String GHOST_VERTEX_COUNT = "ghost-vertices";
    /**
     * Number of result sets that got (possibly) truncated due to an applied query limit
     */
    public static final String TRUNCATED_ENTRY_LISTS = "truncated-results";

    protected final GraphProvider graph;
    protected final VertexScanJob job;

    protected StandardJanusGraphTx tx;
    private IDManager idManager;

    protected VertexJobConverter(JanusGraph graph, VertexScanJob job) {
        Preconditions.checkArgument(job!=null);
        this.graph = new GraphProvider();
        if (graph!=null) this.graph.setGraph(graph);
        this.job = job;
    }

    protected VertexJobConverter(org.janusgraph.graphdb.olap.VertexJobConverter copy) {
        this.graph = copy.graph;
        this.job = copy.job.clone();
        this.tx = copy.tx;
        this.idManager = copy.idManager;
    }

    public static ScanJob convert(JanusGraph graph, VertexScanJob vertexJob) {
        return new org.janusgraph.graphdb.olap.VertexJobConverter(graph,vertexJob);
    }

    public static ScanJob convert(VertexScanJob vertexJob) {
        return new org.janusgraph.graphdb.olap.VertexJobConverter(null,vertexJob);
    }

    public static StandardJanusGraphTx startTransaction(StandardJanusGraph graph) {
        StandardTransactionBuilder txb = graph.buildTransaction().readOnly();
        txb.checkInternalVertexExistence(false);
        txb.dirtyVertexSize(0);
        txb.vertexCacheSize(0);
        return txb.start();
    }

    @Override
    public void workerIterationStart(Configuration jobConfig, Configuration graphConfig, ScanMetrics metrics) {
        try {
            open(graphConfig);
            job.workerIterationStart(graph.get(), jobConfig, metrics);
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    protected void open(Configuration graphConfig) {
        graph.initializeGraph(graphConfig);
        idManager = graph.get().getIDManager();
        tx = startTransaction(graph.get());
    }

    protected void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();
        graph.close();
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        job.workerIterationEnd(metrics);
        close();
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        long vertexId = getVertexId(key);
        if (isGhostVertex(vertexId, entries.get(VERTEX_EXISTS_QUERY))) {
            metrics.incrementCustom(GHOST_VERTEX_COUNT);
            return;
        }
        JanusGraphVertex v = tx.getInternalVertex(vertexId);

        for (Map.Entry<SliceQuery, EntryList> entry : entries.entrySet()) {
            SliceQuery sq = entry.getKey();
            if (sq.equals(VERTEX_EXISTS_QUERY)) continue;
            EntryList entryList = entry.getValue();
            if (entryList.size()>=sq.getLimit()) metrics.incrementCustom(TRUNCATED_ENTRY_LISTS);
//            v.addToQueryCache(sq.updateLimit(Query.NO_LIMIT),entryList); // commented out as not sure what's really going on here
        }
        job.process(v, metrics);
    }

    protected boolean isGhostVertex(long vertexId, EntryList firstEntries) {
        if (idManager.isPartitionedVertex(vertexId) && !idManager.isCanonicalVertexId(vertexId)) return false;

        RelationCache relCache = tx.getEdgeSerializer().parseRelation(
                firstEntries.get(0),true,tx);
        return relCache.typeId != BaseKey.VertexExists.longId();
    }

    @Override
    public List<SliceQuery> getQueries() {
        try {
            QueryContainer qc = new QueryContainer(tx);
            job.getQueries(qc);

            List<SliceQuery> slices = new ArrayList<>();
            slices.add(VERTEX_EXISTS_QUERY);
            slices.addAll(qc.getSliceQueries());
            return slices;
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        return buffer -> !IDManager.VertexIDType.Invisible.is(getVertexId(buffer));
    }

    @Override
    public org.janusgraph.graphdb.olap.VertexJobConverter clone() {
        return new org.janusgraph.graphdb.olap.VertexJobConverter(this);
    }

    protected long getVertexId(StaticBuffer key) {
        return idManager.getKeyID(key);
    }

    public static class GraphProvider {

        private StandardJanusGraph graph=null;
        private boolean provided=false;

        public void setGraph(JanusGraph graph) {
            Preconditions.checkArgument(graph!=null && graph.isOpen(),"Need to provide open graph");
            this.graph = (StandardJanusGraph)graph;
            provided = true;
        }

        public void initializeGraph(Configuration config) {
            if (!provided) {
                this.graph = (StandardJanusGraph) JanusGraphFactory.open((BasicConfiguration) config);
            }
        }

        public void close() {
            if (!provided && null != graph && graph.isOpen()) {
                graph.close();
                graph=null;
            }
        }

        public boolean isProvided() {
            return provided;
        }

        public final StandardJanusGraph get() {
            Preconditions.checkNotNull(graph);
            return graph;
        }


    }

}

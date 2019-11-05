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

package grakn.core.graph.graphdb.olap.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import grakn.core.graph.core.JanusGraph;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.configuration.Configuration;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanJob;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import grakn.core.graph.diskstorage.util.BufferUtil;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.database.management.RelationTypeIndexWrapper;
import grakn.core.graph.graphdb.idmanagement.IDManager;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.olap.QueryContainer;
import grakn.core.graph.graphdb.olap.VertexJobConverter;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class IndexRemoveJob extends IndexUpdateJob implements ScanJob {

    private final VertexJobConverter.GraphProvider graph = new VertexJobConverter.GraphProvider();

    public static final String DELETED_RECORDS_COUNT = "deletes";

    private IndexSerializer indexSerializer;
    private long graphIndexId;
    private IDManager idManager;

    protected IndexRemoveJob(IndexRemoveJob copy) {
        super(copy);
        if (copy.graph.isProvided()) this.graph.setGraph(copy.graph.get());
    }

    public IndexRemoveJob(JanusGraph graph, String indexName, String indexType) {
        super(indexName, indexType);
        this.graph.setGraph(graph);
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        super.workerIterationEnd(metrics);
        graph.close();
    }

    @Override
    public void workerIterationStart(Configuration config, Configuration graphConf, ScanMetrics metrics) {
        graph.initializeGraph(graphConf);
        indexSerializer = graph.get().getIndexSerializer();
        idManager = graph.get().getIDManager();
        try {
            super.workerIterationStart(graph.get(), config, metrics);
        } catch (Throwable e) {
            graph.close();
            throw e;
        }
    }

    @Override
    protected void validateIndexStatus() {
        if (!(index instanceof RelationTypeIndex || index instanceof JanusGraphIndex)) {
            throw new UnsupportedOperationException("Unsupported index found: " + index);
        }
        if (index instanceof JanusGraphIndex) {
            JanusGraphIndex graphIndex = (JanusGraphIndex) index;
            if (graphIndex.isMixedIndex())
                throw new UnsupportedOperationException("Cannot remove mixed indexes through JanusGraph. This can " +
                        "only be accomplished in the indexing system directly.");
            CompositeIndexType indexType = (CompositeIndexType) managementSystem.getSchemaVertex(index).asIndexType();
            graphIndexId = indexType.getID();
        }

        //Must be a relation type index or a composite graph index
        JanusGraphSchemaVertex schemaVertex = managementSystem.getSchemaVertex(index);
        SchemaStatus actualStatus = schemaVertex.getStatus();
        Preconditions.checkArgument(actualStatus == SchemaStatus.DISABLED, "The index [%s] must be disabled before it can be removed", indexName);
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        //The queries are already tailored enough => everything should be removed
        try {
            BackendTransaction mutator = writeTx.getBackendTransaction();
            final List<Entry> deletions;
            if (entries.size() == 1) deletions = Iterables.getOnlyElement(entries.values());
            else {
                final int size = IteratorUtils.stream(entries.values().iterator()).map(List::size).reduce(0, (x, y) -> x + y);
                deletions = new ArrayList<>(size);
                entries.values().forEach(deletions::addAll);
            }
            metrics.incrementCustom(DELETED_RECORDS_COUNT, deletions.size());
            if (isRelationTypeIndex()) {
                mutator.mutateEdges(key, KCVSCache.NO_ADDITIONS, deletions);
            } else {
                mutator.mutateIndex(key, KCVSCache.NO_ADDITIONS, deletions);
            }
        } catch (Exception e) {
            managementSystem.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    @Override
    public List<SliceQuery> getQueries() {
        if (isGlobalGraphIndex()) {
            //Everything
            return ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(1), BufferUtil.oneBuffer(128)));
        } else {
            RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper) index;
            InternalRelationType wrappedType = wrapper.getWrappedType();
            Direction direction = null;
            for (Direction dir : Direction.values()) if (wrappedType.isUnidirected(dir)) direction = dir;

            StandardJanusGraphTx tx = (StandardJanusGraphTx) graph.get().buildTransaction().readOnly().start();
            try {
                QueryContainer qc = new QueryContainer(tx);
                qc.addQuery().type(wrappedType).direction(direction).relations();
                return qc.getSliceQueries();
            } finally {
                tx.rollback();
            }
        }
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        if (isGlobalGraphIndex()) {
            return (k -> {
                try {
                    return indexSerializer.getIndexIdFromKey(k) == graphIndexId;
                } catch (RuntimeException e) {
                    LOG.error("Filtering key {} due to exception", k, e);
                    return false;
                }
            });
        } else {
            return buffer -> !IDManager.VertexIDType.Invisible.is(idManager.getKeyID(buffer));
        }
    }

    @Override
    public IndexRemoveJob clone() {
        return new IndexRemoveJob(this);
    }
}

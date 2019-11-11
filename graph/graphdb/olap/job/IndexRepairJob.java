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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Direction;
import grakn.core.graph.core.BaseVertexQuery;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphException;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.PropertyKey;
import grakn.core.graph.core.RelationType;
import grakn.core.graph.core.schema.JanusGraphIndex;
import grakn.core.graph.core.schema.JanusGraphSchemaType;
import grakn.core.graph.core.schema.RelationTypeIndex;
import grakn.core.graph.core.schema.SchemaAction;
import grakn.core.graph.core.schema.SchemaStatus;
import grakn.core.graph.diskstorage.BackendTransaction;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.StaticBuffer;
import grakn.core.graph.diskstorage.indexing.IndexEntry;
import grakn.core.graph.diskstorage.keycolumnvalue.cache.KCVSCache;
import grakn.core.graph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import grakn.core.graph.graphdb.database.EdgeSerializer;
import grakn.core.graph.graphdb.database.IndexSerializer;
import grakn.core.graph.graphdb.database.management.RelationTypeIndexWrapper;
import grakn.core.graph.graphdb.internal.InternalRelation;
import grakn.core.graph.graphdb.internal.InternalRelationType;
import grakn.core.graph.graphdb.olap.QueryContainer;
import grakn.core.graph.graphdb.olap.VertexScanJob;
import grakn.core.graph.graphdb.olap.job.IndexUpdateJob;
import grakn.core.graph.graphdb.relations.EdgeDirection;
import grakn.core.graph.graphdb.types.CompositeIndexType;
import grakn.core.graph.graphdb.types.IndexType;
import grakn.core.graph.graphdb.types.MixedIndexType;
import grakn.core.graph.graphdb.types.system.BaseLabel;
import grakn.core.graph.graphdb.types.vertices.JanusGraphSchemaVertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class IndexRepairJob extends IndexUpdateJob implements VertexScanJob {

    /**
     * The number of composite-index entries modified or added to the storage
     * backend by this job.
     */
    public static final String ADDED_RECORDS_COUNT = "adds";

    /**
     * The number of mixed-index documents (or whatever idiom is equivalent to the
     * document in the backend implementation) modified by this job
     */
    public static final String DOCUMENT_UPDATES_COUNT = "doc-updates";

    public IndexRepairJob() {
        super();
    }

    protected IndexRepairJob(IndexRepairJob job) {
        super(job);
    }

    public IndexRepairJob(String indexName, String indexType) {
        super(indexName, indexType);
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    @Override
    protected void validateIndexStatus() {
        JanusGraphSchemaVertex schemaVertex = managementSystem.getSchemaVertex(index);
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REINDEX.getApplicableStatus();
        boolean isValidIndex = true;
        String invalidIndexHint;
        if (index instanceof RelationTypeIndex || (index instanceof JanusGraphIndex && ((JanusGraphIndex) index).isCompositeIndex())) {
            SchemaStatus actualStatus = schemaVertex.getStatus();
            isValidIndex = acceptableStatuses.contains(actualStatus);
            invalidIndexHint = String.format(
                    "The index has status %s, but one of %s is required",
                    actualStatus, acceptableStatuses);
        } else {
            Preconditions.checkArgument(index instanceof JanusGraphIndex, "Unexpected index: %s", index);
            JanusGraphIndex graphIndex = (JanusGraphIndex) index;
            Preconditions.checkArgument(graphIndex.isMixedIndex());
            Map<String, SchemaStatus> invalidKeyStatuses = new HashMap<>();
            int acceptableFields = 0;
            for (PropertyKey key : graphIndex.getFieldKeys()) {
                SchemaStatus status = graphIndex.getIndexStatus(key);
                if (status != SchemaStatus.DISABLED && !acceptableStatuses.contains(status)) {
                    isValidIndex = false;
                    invalidKeyStatuses.put(key.name(), status);
                    LOG.warn("Index {} has key {} in an invalid status {}", index, key, status);
                }
                if (acceptableStatuses.contains(status)) acceptableFields++;
            }
            invalidIndexHint = String.format(
                    "The following index keys have invalid status: %s (status must be one of %s)",
                    Joiner.on(",").withKeyValueSeparator(" has status ").join(invalidKeyStatuses), acceptableStatuses);
            if (isValidIndex && acceptableFields == 0) {
                isValidIndex = false;
                invalidIndexHint = "The index does not contain any valid keys";
            }
        }
        Preconditions.checkArgument(isValidIndex, "The index %s is in an invalid state and cannot be indexed. %s", indexName, invalidIndexHint);
        // TODO consider retrieving the current Job object and calling killJob() if !isValidIndex -- would be more efficient than throwing an exception on the first pair processed by each mapper
    }


    @Override
    public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
        try {
            BackendTransaction mutator = writeTx.getBackendTransaction();
            if (index instanceof RelationTypeIndex) {
                RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper) index;
                InternalRelationType wrappedType = wrapper.getWrappedType();
                EdgeSerializer edgeSerializer = writeTx.getEdgeSerializer();
                List<Entry> additions = new ArrayList<>();

                for (Object relation : vertex.query().types(indexRelationTypeName).direction(Direction.OUT).relations()) {
                    InternalRelation janusgraphRelation = (InternalRelation) relation;
                    for (int pos = 0; pos < janusgraphRelation.getArity(); pos++) {
                        if (!wrappedType.isUnidirected(Direction.BOTH) && !wrappedType.isUnidirected(EdgeDirection.fromPosition(pos))) {
                            continue;
                        } //Directionality is not covered
                        Entry entry = edgeSerializer.writeRelation(janusgraphRelation, wrappedType, pos, writeTx);
                        additions.add(entry);
                    }
                }
                StaticBuffer vertexKey = writeTx.getIdManager().getKey(vertex.longId());
                mutator.mutateEdges(vertexKey, additions, KCVSCache.NO_DELETIONS);
                metrics.incrementCustom(ADDED_RECORDS_COUNT, additions.size());
            } else if (index instanceof JanusGraphIndex) {
                IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
                IndexSerializer indexSerializer = graph.getIndexSerializer();
                //Gather elements to index
                List<JanusGraphElement> elements;
                switch (indexType.getElement()) {
                    case VERTEX:
                        elements = ImmutableList.of(vertex);
                        break;
                    case PROPERTY:
                        elements = Lists.newArrayList();
                        for (JanusGraphVertexProperty p : addIndexSchemaConstraint(vertex.query(), indexType).properties()) {
                            elements.add(p);
                        }
                        break;
                    case EDGE:
                        elements = Lists.newArrayList();
                        for (Object e : addIndexSchemaConstraint(vertex.query().direction(Direction.OUT), indexType).edges()) {
                            elements.add((JanusGraphEdge) e);
                        }
                        break;
                    default:
                        throw new AssertionError("Unexpected category: " + indexType.getElement());
                }
                if (indexType.isCompositeIndex()) {
                    for (JanusGraphElement element : elements) {
                        Set<IndexSerializer.IndexUpdate<StaticBuffer, Entry>> updates =
                                indexSerializer.reindexElement(element, (CompositeIndexType) indexType);
                        for (IndexSerializer.IndexUpdate<StaticBuffer, Entry> update : updates) {
                            LOG.debug("Mutating index {}: {}", indexType, update.getEntry());
                            mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
                            metrics.incrementCustom(ADDED_RECORDS_COUNT);
                        }
                    }
                } else {
                    Map<String, Map<String, List<IndexEntry>>> documentsPerStore = new HashMap<>();
                    for (JanusGraphElement element : elements) {
                        indexSerializer.reindexElement(element, (MixedIndexType) indexType, documentsPerStore);
                        metrics.incrementCustom(DOCUMENT_UPDATES_COUNT);
                    }
                    mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
                }

            } else throw new UnsupportedOperationException("Unsupported index found: " + index);
        } catch (Exception e) {
            managementSystem.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {
        if (index instanceof RelationTypeIndex) {
            queries.addQuery().types(indexRelationTypeName).direction(Direction.OUT).relations();
        } else if (index instanceof JanusGraphIndex) {
            IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
            switch (indexType.getElement()) {
                case PROPERTY:
                    addIndexSchemaConstraint(queries.addQuery(), indexType).properties();
                    break;
                case VERTEX:
                    queries.addQuery().properties();
                    queries.addQuery().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).edges();
                    break;
                case EDGE:
                    indexType.hasSchemaTypeConstraint();
                    addIndexSchemaConstraint(queries.addQuery().direction(Direction.OUT), indexType).edges();
                    break;
                default:
                    throw new AssertionError("Unexpected category: " + indexType.getElement());
            }
        } else throw new UnsupportedOperationException("Unsupported index found: " + index);
    }

    @Override
    public IndexRepairJob clone() {
        return new IndexRepairJob(this);
    }

    private static <Q extends BaseVertexQuery> Q addIndexSchemaConstraint(Q query, IndexType indexType) {
        if (indexType.hasSchemaTypeConstraint()) {
            JanusGraphSchemaType constraint = indexType.getSchemaTypeConstraint();
            Preconditions.checkArgument(constraint instanceof RelationType, "Expected constraint to be a " +
                    "relation type: %s", constraint);
            query.types((RelationType) constraint);
        }
        return query;
    }
}

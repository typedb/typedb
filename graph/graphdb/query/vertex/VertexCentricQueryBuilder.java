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

package grakn.core.graph.graphdb.query.vertex;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.JanusGraphVertexQuery;
import grakn.core.graph.core.VertexList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.RelationCategory;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;

import java.util.List;

/**
 * Implementation of JanusGraphVertexQuery that extends BasicVertexCentricQueryBuilder
 * for all the query building and optimization and adds only the execution logic in
 * #constructQuery(RelationCategory). However, there is
 * one important special case: If the constructed query is simple
 * then we use the SimpleVertexQueryProcessor to execute the query instead of the generic QueryProcessor
 * for performance reasons and we compute the result sets differently to make things faster and more memory efficient.
 * <p>
 * The simplified vertex processing only applies to loaded (i.e. non-mutated) vertices. The query can be configured
 * to only included loaded relations in the result set (which is needed, for instance, when computing index deltas in
 * IndexSerialize}) via #queryOnlyLoaded().
 * <p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 */
public class VertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<VertexCentricQueryBuilder> implements JanusGraphVertexQuery<VertexCentricQueryBuilder> {

    /**
     * The base vertex of this query
     */
    private final InternalVertex vertex;

    public VertexCentricQueryBuilder(InternalVertex v) {
        super(v.tx());
        Preconditions.checkNotNull(v);
        this.vertex = v;
    }

    @Override
    protected VertexCentricQueryBuilder getThis() {
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
     * ---------------------------------------------------------------
     */

    protected <Q> Q execute(RelationCategory returnType, ResultConstructor<Q> resultConstructor) {
        BaseVertexCentricQuery bq = super.constructQuery(returnType);
        if (bq.isEmpty()) return resultConstructor.emptyResult();
        if (returnType == RelationCategory.PROPERTY && hasSingleType() && !hasQueryOnlyLoaded()
                && tx.getConfiguration().hasPropertyPrefetching()) {
            //Preload properties
            vertex.query().properties().iterator().hasNext();
        }

        if (isPartitionedVertex(vertex)) {
            List<InternalVertex> vertices = allRequiredRepresentatives(vertex);
            profiler.setAnnotation(QueryProfiler.PARTITIONED_VERTEX_ANNOTATION, true);
            profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION, vertices.size());
            if (vertices.size() > 1) {
                for (BackendQueryHolder<SliceQuery> sq : bq.getQueries()) {
                    tx.executeMultiQuery(vertices, sq.getBackendQuery(), sq.getProfiler());
                }
            }
        } else profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION, 1);
        return resultConstructor.getResult(vertex, bq);
    }

    //#### RELATIONS

    @Override
    public Iterable<JanusGraphEdge> edges() {
        return (Iterable) execute(RelationCategory.EDGE, new RelationConstructor());
    }

    @Override
    public Iterable<JanusGraphVertexProperty> properties() {
        return (Iterable) (isImplicitKeyQuery(RelationCategory.PROPERTY) ?
                executeImplicitKeyQuery(vertex) :
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public Iterable<JanusGraphRelation> relations() {
        return (Iterable) (isImplicitKeyQuery(RelationCategory.RELATION) ?
                executeImplicitKeyQuery(vertex) :
                execute(RelationCategory.RELATION, new RelationConstructor()));
    }

    //#### VERTICES

    @Override
    public Iterable<JanusGraphVertex> vertices() {
        return execute(RelationCategory.EDGE, new VertexConstructor());
    }

    @Override
    public VertexList vertexIds() {
        return execute(RelationCategory.EDGE, new VertexIdConstructor());
    }

}

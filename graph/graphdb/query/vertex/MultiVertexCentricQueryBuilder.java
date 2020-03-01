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
import com.google.common.collect.Sets;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphMultiVertexQuery;
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexProperty;
import grakn.core.graph.core.VertexList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.RelationCategory;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of JanusGraphMultiVertexQuery that extends BasicVertexCentricQueryBuilder
 * for all the query building and optimization and adds only the execution logic in
 * #execute(RelationCategory, BasicVertexCentricQueryBuilder.ResultConstructor).
 * <p>
 * All other methods just prepare or transform that result set to fit the particular method semantics.
 */
public class MultiVertexCentricQueryBuilder extends BasicVertexCentricQueryBuilder<MultiVertexCentricQueryBuilder> implements JanusGraphMultiVertexQuery<MultiVertexCentricQueryBuilder> {

    /**
     * The base vertices of this query
     */
    private final Set<InternalVertex> vertices;

    public MultiVertexCentricQueryBuilder(StandardJanusGraphTx tx) {
        super(tx);
        vertices = Sets.newHashSet();
    }

    @Override
    protected MultiVertexCentricQueryBuilder getThis() {
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Construction
     * ---------------------------------------------------------------
     */

    @Override
    public JanusGraphMultiVertexQuery addVertex(Vertex vertex) {
        vertices.add(((InternalVertex) vertex).it());
        return this;
    }

    @Override
    public JanusGraphMultiVertexQuery addAllVertices(Collection<? extends Vertex> vertices) {
        for (Vertex v : vertices) addVertex(v);
        return this;
    }

    /* ---------------------------------------------------------------
     * Query Execution
     * ---------------------------------------------------------------
     */

    /**
     * Constructs the BaseVertexCentricQuery through BasicVertexCentricQueryBuilder#constructQuery(RelationCategory).
     * If the query asks for an implicit key, the resulting map is computed and returned directly.
     * If the query is empty, a map that maps each vertex to an empty list is returned.
     * Otherwise, the query is executed for all vertices through the transaction which will effectively
     * pre-load the return result sets into the associated CacheVertex or
     * don't do anything at all if the vertex is new (and hence no edges in the storage backend).
     * After that, a map is constructed that maps each vertex to the corresponding VertexCentricQuery and wrapped
     * into a QueryProcessor. Hence, upon iteration the query will be executed like any other VertexCentricQuery
     * with the performance difference that the SliceQueries will have already been preloaded and not further
     * calls to the storage backend are needed.
     *
     * @param returnType
     * @return
     */
    protected <Q> Map<JanusGraphVertex, Q> execute(RelationCategory returnType, ResultConstructor<Q> resultConstructor) {
        Preconditions.checkArgument(!vertices.isEmpty(), "Need to add at least one vertex to query");
        Map<JanusGraphVertex, Q> result = new HashMap<>(vertices.size());
        BaseVertexCentricQuery bq = super.constructQuery(returnType);
        profiler.setAnnotation(QueryProfiler.MULTIQUERY_ANNOTATION, true);
        profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION, vertices.size());
        if (!bq.isEmpty()) {
            for (BackendQueryHolder<SliceQuery> sq : bq.getQueries()) {
                Set<InternalVertex> adjVertices = Sets.newHashSet(vertices);
                for (InternalVertex v : vertices) {
                    if (isPartitionedVertex(v)) {
                        profiler.setAnnotation(QueryProfiler.PARTITIONED_VERTEX_ANNOTATION, true);
                        adjVertices.remove(v);
                        adjVertices.addAll(allRequiredRepresentatives(v));
                    }
                }
                //Overwrite with more accurate size accounting for partitioned vertices
                profiler.setAnnotation(QueryProfiler.NUMVERTICES_ANNOTATION, adjVertices.size());
                tx.executeMultiQuery(adjVertices, sq.getBackendQuery(), sq.getProfiler());
            }
            for (InternalVertex v : vertices) {
                result.put(v, resultConstructor.getResult(v, bq));
            }
        } else {
            for (JanusGraphVertex v : vertices) {
                result.put(v, resultConstructor.emptyResult());
            }
        }
        return result;
    }

    private Map<JanusGraphVertex, Iterable<? extends JanusGraphRelation>> executeImplicitKeyQuery() {
        return new HashMap<JanusGraphVertex, Iterable<? extends JanusGraphRelation>>(vertices.size()) {{
            for (InternalVertex v : vertices) put(v, executeImplicitKeyQuery(v));
        }};
    }

    @Override
    public Map<JanusGraphVertex, Iterable<JanusGraphEdge>> edges() {
        return (Map) execute(RelationCategory.EDGE, new RelationConstructor());
    }

    @Override
    public Map<JanusGraphVertex, Iterable<JanusGraphVertexProperty>> properties() {
        return (Map) (isImplicitKeyQuery(RelationCategory.PROPERTY) ?
                executeImplicitKeyQuery() :
                execute(RelationCategory.PROPERTY, new RelationConstructor()));
    }

    @Override
    public void preFetch() {
        profiler.setAnnotation(QueryProfiler.MULTIPREFETCH_ANNOTATION, true);
        properties();
    }

    @Override
    public Map<JanusGraphVertex, Iterable<JanusGraphRelation>> relations() {
        return (Map) (isImplicitKeyQuery(RelationCategory.RELATION) ?
                executeImplicitKeyQuery() :
                execute(RelationCategory.RELATION, new RelationConstructor()));
    }

    @Override
    public Map<JanusGraphVertex, Iterable<JanusGraphVertex>> vertices() {
        return execute(RelationCategory.EDGE, new VertexConstructor());
    }

    @Override
    public Map<JanusGraphVertex, VertexList> vertexIds() {
        return execute(RelationCategory.EDGE, new VertexIdConstructor());
    }

}

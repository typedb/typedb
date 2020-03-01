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
import grakn.core.graph.core.JanusGraphRelation;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.internal.OrderList;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.ElementQuery;
import grakn.core.graph.graphdb.query.condition.Condition;
import grakn.core.graph.graphdb.relations.RelationComparator;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Comparator;
import java.util.List;

/**
 * A vertex-centric query which implements ElementQuery so that it can be executed by
 * QueryProcessor. Most of the query definition
 * is in the extended BaseVertexCentricQuery - this class only adds the base vertex to the mix.
 */
public class VertexCentricQuery extends BaseVertexCentricQuery implements ElementQuery<JanusGraphRelation, SliceQuery> {

    private final InternalVertex vertex;

    public VertexCentricQuery(InternalVertex vertex, Condition<JanusGraphRelation> condition,
                              Direction direction,
                              List<BackendQueryHolder<SliceQuery>> queries,
                              OrderList orders,
                              int limit) {
        super(condition, direction, queries, orders, limit);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public VertexCentricQuery(InternalVertex vertex, BaseVertexCentricQuery base) {
        super(base);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    /**
     * Constructs an empty query
     *
     * @param vertex
     */
    protected VertexCentricQuery(InternalVertex vertex) {
        super();
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public static VertexCentricQuery emptyQuery(InternalVertex vertex) {
        return new VertexCentricQuery(vertex);
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public Comparator getSortOrder() {
        return new RelationComparator(vertex, getOrders());
    }

    @Override
    public boolean hasDuplicateResults() {
        return false; //We wanna count self-loops twice
    }

    @Override
    public String toString() {
        return vertex + super.toString();
    }

}

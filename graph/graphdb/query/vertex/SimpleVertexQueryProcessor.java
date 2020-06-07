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
import grakn.core.graph.core.VertexList;
import grakn.core.graph.diskstorage.Entry;
import grakn.core.graph.diskstorage.EntryList;
import grakn.core.graph.diskstorage.keycolumnvalue.SliceQuery;
import grakn.core.graph.graphdb.database.EdgeSerializer;
import grakn.core.graph.graphdb.internal.InternalVertex;
import grakn.core.graph.graphdb.query.BackendQueryHolder;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.transaction.RelationConstructor;
import grakn.core.graph.graphdb.transaction.StandardJanusGraphTx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This is an optimization of specifically for VertexCentricQuery that addresses the special but
 * common case that the query is simple (i.e. comprised of only one sub-query and that query is fitted, i.e. does not require
 * in memory filtering). Under these assumptions we can remove a lot of the steps in grakn.core.graph.graphdb.query.QueryProcessor:
 * merging of result sets, in-memory filtering and the object instantiation required for in-memory filtering.
 * <p>
 * With those complexities removed, the query processor can be much simpler which makes it a lot faster and less
 * memory intense.
 * <p>
 * IMPORTANT: This Iterable is not thread-safe.
 */
public class SimpleVertexQueryProcessor implements Iterable<Entry> {

    private final VertexCentricQuery query;
    private final StandardJanusGraphTx tx;
    private final EdgeSerializer edgeSerializer;
    private final InternalVertex vertex;
    private final QueryProfiler profiler;

    private SliceQuery sliceQuery;

    public SimpleVertexQueryProcessor(VertexCentricQuery query, StandardJanusGraphTx tx) {
        Preconditions.checkArgument(query.isSimple());
        this.query = query;
        this.tx = tx;
        BackendQueryHolder<SliceQuery> bqh = query.getSubQuery(0);
        this.sliceQuery = bqh.getBackendQuery();
        this.profiler = bqh.getProfiler();
        this.vertex = query.getVertex();
        this.edgeSerializer = tx.getEdgeSerializer();
    }

    @Override
    public Iterator<Entry> iterator() {
        Iterator<Entry> iterator;
        //If there is a limit we need to wrap the basic iterator in a LimitAdjustingIterator which ensures the right number
        //of elements is returned. Otherwise we just return the basic iterator.
        if (sliceQuery.hasLimit() && sliceQuery.getLimit() != query.getLimit()) {
            iterator = new LimitAdjustingIterator();
        } else {
            iterator = getBasicIterator();
        }
        return iterator;
    }

    /**
     * Converts the entries from this query result into actual JanusGraphRelation.
     */
    public Iterable<JanusGraphRelation> relations() {
        return RelationConstructor.readRelation(vertex, this, tx);
    }

    /**
     * Returns the list of adjacent vertex ids for this query. By reading those ids
     * from the entries directly (without creating objects) we get much better performance.
     */
    public VertexList vertexIds() {
        List<Long> list = new ArrayList<>();
        long previousId = 0;

        Iterable<Long> vertexIds = StreamSupport.stream(this.spliterator(), false)
                .map(entry -> edgeSerializer.readRelation(entry, true, tx).getOtherVertexId())
                .collect(Collectors.toList());

        for (Long id : vertexIds) {
            list.add(id);
            if (id >= previousId && previousId >= 0) previousId = id;
            else previousId = -1;
        }
        return new VertexLongList(tx, list, previousId >= 0);
    }

    /**
     * Executes the query by executing its on SliceQuery sub-query.
     *
     */
    private Iterator<Entry> getBasicIterator() {
        EntryList result = vertex.loadRelations(sliceQuery, query -> QueryProfiler.profile(profiler, query, q -> tx.getGraph().edgeQuery(vertex.longId(), q, tx.getBackendTransaction())));
        return result.iterator();
    }


    private final class LimitAdjustingIterator extends grakn.core.graph.graphdb.query.LimitAdjustingIterator<Entry> {

        private LimitAdjustingIterator() {
            super(query.getLimit(), sliceQuery.getLimit());
        }

        @Override
        public Iterator<Entry> getNewIterator(int newLimit) {
            if (newLimit > sliceQuery.getLimit()) {
                sliceQuery = sliceQuery.updateLimit(newLimit);
            }
            return getBasicIterator();
        }
    }

}

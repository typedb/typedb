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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import grakn.core.graph.core.BaseVertexQuery;
import grakn.core.graph.core.JanusGraphMultiVertexQuery;
import grakn.core.graph.core.JanusGraphProperty;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexQuery;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.Query;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import grakn.core.graph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JanusGraphPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element, E>, Profiling, MultiQueriable<Element, E> {

    private boolean initialized = false;
    private boolean useMultiQuery = false;
    private Map<JanusGraphVertex, Iterable<? extends JanusGraphProperty>> multiQueryResults = null;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;

    public JanusGraphPropertiesStep(PropertiesStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getPropertyKeys());
        originalStep.getLabels().forEach(this::addLabel);
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    private <Q extends BaseVertexQuery> Q makeQuery(Q query) {
        String[] keys = getPropertyKeys();
        query.keys(keys);
        for (HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    private Iterator<E> convertIterator(Iterable<? extends JanusGraphProperty> iterable) {
        if (getReturnType().forProperties()) {
            return (Iterator<E>) iterable.iterator();
        }
        return (Iterator<E>) Iterators.transform(iterable.iterator(), Property::value);
    }

    private void initialize() {
        initialized = true;

        if (!starts.hasNext()) throw FastNoSuchElementException.instance();
        List<Traverser.Admin<Element>> elements = new ArrayList<>();
        starts.forEachRemaining(elements::add);
        starts.add(elements.iterator());

        useMultiQuery = useMultiQuery && elements.stream().allMatch(e -> e.get() instanceof Vertex);

        if (useMultiQuery) {
            initializeMultiQuery(elements);
        }
    }

    /**
     * This initialisation method is called the first time this instance is used and also when
     * an attempt to retrieve a vertex from the cached multiQuery results doesn't find an entry.
     *
     * @param list list of vertices with which to initialise the multiQuery
     */
    private void initializeMultiQuery(List<Traverser.Admin<Element>> list) {
        JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
        list.forEach(v -> multiQuery.addVertex((Vertex) v.get()));
        makeQuery(multiQuery);

        Map<JanusGraphVertex, Iterable<? extends JanusGraphProperty>> results = multiQuery.properties();
        if (multiQueryResults == null) {
            multiQueryResults = results;
        } else {
            multiQueryResults.putAll(results);
        }
        initialized = true;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Element> traverser) {
        if (useMultiQuery) { //it is guaranteed that all elements are vertices
            if (multiQueryResults == null || !multiQueryResults.containsKey(traverser.get())) {
                initializeMultiQuery(Arrays.asList(traverser));
            }
            return convertIterator(multiQueryResults.get(traverser.get()));
        } else if (traverser.get() instanceof JanusGraphVertex || traverser.get() instanceof WrappedVertex) {
            JanusGraphVertexQuery query = makeQuery((JanusGraphTraversalUtil.getJanusGraphVertex(traverser)).query());
            return convertIterator(query.properties());
        } else {
            //It is some other element (edge or vertex property)
            Iterator<E> iterator;
            if (getReturnType().forValues()) {
                iterator = traverser.get().values(getPropertyKeys());
            } else {
                //HasContainers don't apply => empty result set
                if (!hasContainers.isEmpty()) return Collections.emptyIterator();
                iterator = (Iterator<E>) traverser.get().properties(getPropertyKeys());
            }
            if (limit != Query.NO_LIMIT) iterator = Iterators.limit(iterator, limit);
            return iterator;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public JanusGraphPropertiesStep<E> clone() {
        JanusGraphPropertiesStep<E> clone = (JanusGraphPropertiesStep<E>) super.clone();
        clone.initialized = false;
        return clone;
    }

    /*
    ===== HOLDER =====
     */

    private final List<HasContainer> hasContainers;
    private int limit;
    private final List<OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public List<HasContainer> addLocalAll(Iterable<HasContainer> has) {
        throw new UnsupportedOperationException("addLocalAll is not supported for properties step.");
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new HasStepFolder.OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(List<HasContainer> hasContainers, String key, Order order) {
        throw new UnsupportedOperationException("LocalOrderBy is not supported for properties step.");
    }

    @Override
    public void setLimit(int low, int high) {
        Preconditions.checkArgument(low == 0, "Offset is not supported for properties step.");
        this.limit = high;
    }

    @Override
    public void setLocalLimit(List<HasContainer> hasContainers, int low, int high) {
        throw new UnsupportedOperationException("LocalLimit is not supported for properties step.");
    }

    @Override
    public int getLowLimit() {
        throw new UnsupportedOperationException("getLowLimit is not supported for properties step.");
    }

    @Override
    public int getLocalLowLimit(List<HasContainer> hasContainers) {
        throw new UnsupportedOperationException("getLocalLowLimit is not supported for properties step.");
    }

    @Override
    public int getHighLimit() {
        return this.limit;
    }

    @Override
    public int getLocalHighLimit(List<HasContainer> hasContainers) {
        throw new UnsupportedOperationException("getLocalHighLimit is not supported for properties step.");
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : StringFactory.stepString(this, this.hasContainers);
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

}

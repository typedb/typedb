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
import com.google.common.collect.Sets;
import grakn.core.graph.core.BaseVertexQuery;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphMultiVertexQuery;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.core.JanusGraphVertexQuery;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.Query;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import grakn.core.graph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep.RepeatEndStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class JanusGraphVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex, E>, Profiling, MultiQueriable<Vertex, E> {

    private boolean initialized = false;
    private boolean useMultiQuery = false;
    private boolean batchPropertyPrefetching = false;
    private Map<JanusGraphVertex, Iterable<? extends JanusGraphElement>> multiQueryResults = null;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private int txVertexCacheSize = 20000;
    private JanusGraphMultiQueryStep parentMultiQueryStep;

    public JanusGraphVertexStep(VertexStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(), originalStep.getEdgeLabels());
        originalStep.getLabels().forEach(this::addLabel);
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    public void setBatchPropertyPrefetching(boolean batchPropertyPrefetching) {
        this.batchPropertyPrefetching = batchPropertyPrefetching;
    }

    public void setTxVertexCacheSize(int txVertexCacheSize) {
        this.txVertexCacheSize = txVertexCacheSize;
    }

    private <Q extends BaseVertexQuery> Q makeQuery(Q query) {
        query.labels(getEdgeLabels());
        query.direction(getDirection());
        for (HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (limit != BaseQuery.NO_LIMIT) query.limit(limit);
        ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
        return query;
    }

    private void initialize() {
        initialized = true;
        if (useMultiQuery) {
            setParentMultiQueryStep();

            if (!starts.hasNext()) {
                throw FastNoSuchElementException.instance();
            }
            final List<Admin<Vertex>> vertices = new ArrayList<>();
            starts.forEachRemaining(vertices::add);
            starts.add(vertices.iterator());
            initializeMultiQuery(vertices);
        }
    }

    /**
     * This initialisation method is called the first time this instance is used and also when
     * an attempt to retrieve a vertex from the cached multiQuery results doesn't find an entry.
     * If initialised with just a single vertex this might be a drip feed from a parent so it
     * will additionally include any cached starts the parent step may have.
     *
     * @param vertices A list of vertices with which to initialise the multiQuery
     */
    private void initializeMultiQuery(List<Admin<Vertex>> vertices) {
        List<Admin<Vertex>> parentStarts = new ArrayList<>();
        if (vertices.size() == 1 && parentMultiQueryStep != null) {
            parentStarts = parentMultiQueryStep.getCachedStarts();
        }
        JanusGraphMultiVertexQuery multiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
        vertices.forEach(v -> multiQuery.addVertex(v.get()));
        parentStarts.forEach(v -> multiQuery.addVertex(v.get()));
        makeQuery(multiQuery);

        Map<JanusGraphVertex, Iterable<? extends JanusGraphElement>> results = (Vertex.class.isAssignableFrom(getReturnClass())) ? multiQuery.vertices() : multiQuery.edges();
        if (multiQueryResults == null) {
            multiQueryResults = results;
        } else {
            multiQueryResults.putAll(results);
        }
    }

    /**
     * Many parent traversals drip feed their start vertices in one at a time. To best exploit
     * the multiQuery we need to load all possible starts in one go so this method will attempt
     * to find a JanusGraphMultiQueryStep with the starts of the parent, and if found cache it.
     */
    private void setParentMultiQueryStep() {
        Step firstStep = traversal.getStartStep();
        while (firstStep instanceof StartStep || firstStep instanceof SideEffectStep) {
            // Want the next step if this is a side effect
            firstStep = firstStep.getNextStep();
        }
        if (this.equals(firstStep)) {
            Step<?, ?> parentStep = traversal.getParent().asStep();
            if (JanusGraphTraversalUtil.isMultiQueryCompatibleStep(parentStep)) {
                Step<?, ?> parentPreviousStep = parentStep.getPreviousStep();
                if (parentStep instanceof RepeatStep) {
                    RepeatStep repeatStep = (RepeatStep) parentStep;
                    List<RepeatEndStep> repeatEndSteps = TraversalHelper.getStepsOfClass(RepeatEndStep.class, repeatStep.getRepeatTraversal());
                    if (repeatEndSteps.size() == 1) {
                        parentPreviousStep = repeatEndSteps.get(0).getPreviousStep();
                    }
                }
                if (parentPreviousStep instanceof ProfileStep) {
                    parentPreviousStep = parentPreviousStep.getPreviousStep();
                }
                if (parentPreviousStep instanceof JanusGraphMultiQueryStep) {
                    parentMultiQueryStep = (JanusGraphMultiQueryStep) parentPreviousStep;
                }
            }
        }
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(Traverser.Admin<Vertex> traverser) {

        Iterable<? extends JanusGraphElement> result;

        if (useMultiQuery) {
            if (multiQueryResults == null || !multiQueryResults.containsKey(traverser.get())) {
                initializeMultiQuery(Arrays.asList(traverser));
            }
            result = multiQueryResults.get(traverser.get());
        } else {
            final JanusGraphVertexQuery query = makeQuery((JanusGraphTraversalUtil.getJanusGraphVertex(traverser)).query());
            result = (Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices() : query.edges();
        }

        if (batchPropertyPrefetching) {
            Set<Vertex> vertices = Sets.newHashSet();
            result.forEach(v -> {
                if (vertices.size() < txVertexCacheSize) {
                    vertices.add((Vertex) v);
                }
            });

            // If there are multiple vertices then fetch the properties for all of them in a single multiQuery to
            // populate the vertex cache so subsequent queries of properties don't have to go to the storage back end
            if (vertices.size() > 1) {
                JanusGraphMultiVertexQuery propertyMultiQuery = JanusGraphTraversalUtil.getTx(traversal).multiQuery();
                ((BasicVertexCentricQueryBuilder) propertyMultiQuery).profiler(queryProfiler);
                propertyMultiQuery.addAllVertices(vertices).preFetch();
            }
        }

        return (Iterator<E>) result.iterator();
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public JanusGraphVertexStep<E> clone() {
        JanusGraphVertexStep<E> clone = (JanusGraphVertexStep<E>) super.clone();
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
        HasStepFolder.splitAndP(hasContainers, has);
    }

    @Override
    public List<HasContainer> addLocalAll(Iterable<HasContainer> has) {
        throw new UnsupportedOperationException("addLocalAll is not supported for graph vertex step.");
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(List<HasContainer> hasContainers, String key, Order order) {
        throw new UnsupportedOperationException("localOrderBy is not supported for graph vertex step.");
    }

    @Override
    public void setLimit(int low, int high) {
        Preconditions.checkArgument(low == 0, "Offset is not supported for properties step.");
        this.limit = high;
    }

    @Override
    public void setLocalLimit(List<HasContainer> hasContainers, int low, int high) {
        throw new UnsupportedOperationException("setLocalLimit is not supported for graph vertex step.");
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
        throw new UnsupportedOperationException("getLocalHighLimit is not supported for graph vertex step.");
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

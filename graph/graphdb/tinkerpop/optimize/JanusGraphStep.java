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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import grakn.core.graph.core.JanusGraphEdge;
import grakn.core.graph.core.JanusGraphElement;
import grakn.core.graph.core.JanusGraphQuery;
import grakn.core.graph.core.JanusGraphTransaction;
import grakn.core.graph.core.JanusGraphVertex;
import grakn.core.graph.graphdb.internal.ElementCategory;
import grakn.core.graph.graphdb.query.BaseQuery;
import grakn.core.graph.graphdb.query.JanusGraphPredicate;
import grakn.core.graph.graphdb.query.graph.GraphCentricQuery;
import grakn.core.graph.graphdb.query.graph.GraphCentricQueryBuilder;
import grakn.core.graph.graphdb.query.profile.QueryProfiler;
import grakn.core.graph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import grakn.core.graph.graphdb.util.MultiDistinctOrderedIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class JanusGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasStepFolder<S, E>, Profiling, HasContainerHolder {

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final Map<List<HasContainer>, QueryInfo> hasLocalContainers = new LinkedHashMap<>();
    private int lowLimit = 0;
    private int highLimit = BaseQuery.NO_LIMIT;
    private final List<OrderEntry> orders = new ArrayList<>();
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;


    public JanusGraphStep(GraphStep<S, E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.isStartStep(), originalStep.getIds());
        originalStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> {
            if (this.ids == null) {
                return Collections.emptyIterator();
            } else if (this.ids.length > 0) {
                Graph graph = (Graph) traversal.asAdmin().getGraph().get();
                return iteratorList((Iterator) graph.vertices(this.ids));
            }
            if (hasLocalContainers.isEmpty()) {
                hasLocalContainers.put(new ArrayList<>(), new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
            }
            JanusGraphTransaction tx = JanusGraphTraversalUtil.getTx(traversal);
            GraphCentricQuery globalQuery = buildGlobalGraphCentricQuery(tx);

            Multimap<Integer, GraphCentricQuery> queries = ArrayListMultimap.create();
            if (globalQuery != null && !globalQuery.getSubQuery(0).getBackendQuery().isEmpty()) {
                queries.put(0, globalQuery);
            } else {
                hasLocalContainers.entrySet().forEach(c -> queries.put(c.getValue().getLowLimit(), buildGraphCentricQuery(tx, c)));
            }

            GraphCentricQueryBuilder builder = (GraphCentricQueryBuilder) tx.query();
            List<Iterator<E>> responses = new ArrayList<>();
            queries.entries().forEach(q -> executeGraphCentryQuery(builder, responses, q));

            return new MultiDistinctOrderedIterator<E>(lowLimit, highLimit, responses, orders);
        });
    }

    private GraphCentricQuery buildGlobalGraphCentricQuery(JanusGraphTransaction tx) {
        //If a query have a local offset or have a local order without a global order and if a query have a limit lower than the global different from other query we can not build globalquery
        Iterator<QueryInfo> itQueryInfo = hasLocalContainers.values().iterator();
        QueryInfo queryInfo = itQueryInfo.next();
        if (queryInfo.getLowLimit() > 0 || orders.isEmpty() && !queryInfo.getOrders().isEmpty()) {
            return null;
        }
        Integer limit = queryInfo.getHighLimit();
        while (itQueryInfo.hasNext()) {
            queryInfo = itQueryInfo.next();
            if (queryInfo.getLowLimit() > 0 || (orders.isEmpty() && !queryInfo.getOrders().isEmpty()) || (queryInfo.getHighLimit() < highLimit && !limit.equals(queryInfo.getHighLimit()))) {
                return null;
            }
        }
        JanusGraphQuery query = tx.query();
        for (List<HasContainer> localContainers : hasLocalContainers.keySet()) {
            JanusGraphQuery localQuery = tx.query();
            addConstraint(localQuery, localContainers);
            query.or(localQuery);
        }
        for (OrderEntry order : orders) query.orderBy(order.key, order.order);
        if (highLimit != BaseQuery.NO_LIMIT || limit != BaseQuery.NO_LIMIT) query.limit(Math.min(limit, highLimit));
        Preconditions.checkArgument(query instanceof GraphCentricQueryBuilder);
        GraphCentricQueryBuilder centricQueryBuilder = ((GraphCentricQueryBuilder) query);
        centricQueryBuilder.profiler(queryProfiler);
        GraphCentricQuery graphCentricQuery = centricQueryBuilder.constructQuery(Vertex.class.isAssignableFrom(this.returnClass) ? ElementCategory.VERTEX : ElementCategory.EDGE);
        return graphCentricQuery;
    }

    private void addConstraint(JanusGraphQuery query, List<HasContainer> localContainers) {
        for (HasContainer condition : hasContainers) {
            query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
        }
        for (HasContainer condition : localContainers) {
            query.has(condition.getKey(), JanusGraphPredicate.Converter.convert(condition.getBiPredicate()), condition.getValue());
        }
    }

    private GraphCentricQuery buildGraphCentricQuery(JanusGraphTransaction tx, Entry<List<HasContainer>, QueryInfo> containers) {
        JanusGraphQuery query = tx.query();
        addConstraint(query, containers.getKey());
        List<OrderEntry> realOrders = orders.isEmpty() ? containers.getValue().getOrders() : orders;
        for (OrderEntry order : realOrders) {
            query.orderBy(order.key, order.order);
        }
        if (highLimit != BaseQuery.NO_LIMIT || containers.getValue().getHighLimit() != BaseQuery.NO_LIMIT) {
            query.limit(Math.min(containers.getValue().getHighLimit(), highLimit));
        }
        Preconditions.checkArgument(query instanceof GraphCentricQueryBuilder);
        GraphCentricQueryBuilder centricQueryBuilder = ((GraphCentricQueryBuilder) query);
        centricQueryBuilder.profiler(queryProfiler);
        GraphCentricQuery graphCentricQuery = centricQueryBuilder.constructQuery(Vertex.class.isAssignableFrom(this.returnClass) ? ElementCategory.VERTEX : ElementCategory.EDGE);
        return graphCentricQuery;
    }

    private void executeGraphCentryQuery(GraphCentricQueryBuilder builder, List<Iterator<E>> responses,
                                         Entry<Integer, GraphCentricQuery> query) {
        Class<? extends JanusGraphElement> classe = Vertex.class.isAssignableFrom(this.returnClass) ? JanusGraphVertex.class : JanusGraphEdge.class;
        Iterator<E> response = (Iterator<E>) builder.iterables(query.getValue(), classe).iterator();
        long i = 0;
        while (i < query.getKey() && response.hasNext()) {
            response.next();
            i++;
        }
        responses.add(response);
    }

    @Override
    public String toString() {
        if (hasLocalContainers.isEmpty() && hasContainers.isEmpty()) {
            return super.toString();
        }
        if (hasLocalContainers.isEmpty()) {
            return StringFactory.stepString(this, Arrays.toString(this.ids), hasContainers);
        }
        if (hasLocalContainers.size() == 1) {
            List<HasContainer> containers = new ArrayList<>(hasContainers);
            containers.addAll(hasLocalContainers.keySet().iterator().next());
            return StringFactory.stepString(this, Arrays.toString(this.ids), containers);
        }
        StringBuilder sb = new StringBuilder();
        if (!hasContainers.isEmpty()) {
            sb.append(StringFactory.stepString(this, Arrays.toString(ids), hasContainers)).append(".");
        }
        sb.append("Or(");
        Iterator<List<HasContainer>> itContainers = this.hasLocalContainers.keySet().iterator();
        sb.append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
        while (itContainers.hasNext()) {
            sb.append(",").append(StringFactory.stepString(this, Arrays.toString(this.ids), itContainers.next()));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void addAll(Iterable<HasContainer> has) {
        HasStepFolder.splitAndP(hasContainers, has);
    }

    @Override
    public List<HasContainer> addLocalAll(Iterable<HasContainer> has) {
        List<HasContainer> containers = HasStepFolder.splitAndP(new ArrayList<>(), has);
        hasLocalContainers.put(containers, new QueryInfo(new ArrayList<>(), 0, BaseQuery.NO_LIMIT));
        return containers;
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key, order));
    }

    @Override
    public void localOrderBy(List<HasContainer> containers, String key, Order order) {
        hasLocalContainers.get(containers).getOrders().add(new OrderEntry(key, order));
    }

    @Override
    public void setLimit(int low, int high) {
        this.lowLimit = low;
        this.highLimit = high;
    }

    @Override
    public void setLocalLimit(List<HasContainer> containers, int low, int high) {
        hasLocalContainers.replace(containers, hasLocalContainers.get(containers).setLowLimit(low).setHighLimit(high));
    }

    @Override
    public int getLowLimit() {
        return this.lowLimit;
    }

    @Override
    public int getLocalLowLimit(List<HasContainer> containers) {
        return hasLocalContainers.get(containers).getLowLimit();
    }

    @Override
    public int getHighLimit() {
        return this.highLimit;
    }

    @Override
    public int getLocalHighLimit(List<HasContainer> containers) {
        return hasLocalContainers.get(containers).getHighLimit();
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        List<HasContainer> toReturn = new ArrayList<>(this.hasContainers);
        this.hasLocalContainers.keySet().stream().forEach(l -> l.stream().forEach(toReturn::add));
        return toReturn;
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        this.addAll(Collections.singleton(hasContainer));
    }

    public List<OrderEntry> getOrders() {
        return orders;
    }

    private <A extends Element> Iterator<A> iteratorList(Iterator<A> iterator) {
        List<A> list = new ArrayList<>();
        while (iterator.hasNext()) {
            A e = iterator.next();
            if (HasContainer.testAll(e, this.getHasContainers())) {
                list.add(e);
            }
        }
        return list.iterator();
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (hasContainers != null ? this.hasContainers.hashCode() : 0);
        result = 31 * result + (hasLocalContainers != null ? this.hasLocalContainers.hashCode() : 0);
        result = 31 * result + lowLimit;
        result = 31 * result + highLimit;
        result = 31 * result + (orders != null ? orders.hashCode() : 0);
        return result;
    }
}


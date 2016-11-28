/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin;

import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.query.match.MatchOrder;
import ai.grakn.util.ErrorMessage;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A class for building gremlin traversals from patterns.
 * <p>
 * A {@code Query} is constructed from a single {@code Pattern.Conjunction}. The conjunction is transformed into
 * disjunctive normal form and then an {@code InnerQuery} is constructed from each disjunction component. This allows
 * each {@code InnerQuery} to be described by a single gremlin traversal.
 * <p>
 * The {@code Query} returns a list of gremlin traversals, whose results are combined by {@code MatchQueryImpl} to
 * maintain any requested ordering.
 */
public class GremlinQuery {

    protected final Logger LOG = LoggerFactory.getLogger(GremlinQuery.class);

    private final GraknGraph graph;
    private final Collection<ConjunctionQuery> innerQueries;
    private final ImmutableSet<String> names;
    private final Optional<MatchOrder> order;

    /**
     * @param graph the graph to execute the query on
     * @param pattern a pattern to find in the graph
     * @param names the variable names to select
     * @param order an optional ordering
     */
    public GremlinQuery(GraknGraph graph, PatternAdmin pattern, ImmutableSet<String> names, Optional<MatchOrder> order) {
        Collection<Conjunction<VarAdmin>> patterns = pattern.getDisjunctiveNormalForm().getPatterns();

        if (graph == null) {
            throw new IllegalStateException(ErrorMessage.NO_GRAPH.getMessage());
        }

        this.graph = graph;
        this.names = names;
        this.order = order;

        innerQueries = patterns.stream().map(ConjunctionQuery::new).collect(toList());
    }

    /**
     * Get a close-to-optimal traversal plan to execute this query
     */
    public GraqlTraversal optimalTraversal() {
        return GraqlTraversal.semiOptimal(graph, innerQueries);
    }

    /**
     * @return a gremlin traversal to execute to find results
     */
    public GraphTraversal<Vertex, Map<String, Vertex>> getTraversal() {
        GraqlTraversal graqlTraversal = optimalTraversal();

        LOG.debug("Created query plan");
        LOG.debug(graqlTraversal.toString());

        // Because 'union' accepts an array, we can't use generics...
        //noinspection unchecked
        GraphTraversal<Vertex, Map<String, Vertex>> traversal = graqlTraversal.getGraphTraversal();

        order.ifPresent(o -> o.orderTraversal(traversal));

        String[] namesArray = names.toArray(new String[names.size()]);

        // Must provide three arguments in order to pass an array to .select
        // If ordering, select the variable to order by as well
        if (order.isPresent()) {
            traversal.select(order.get().getVar(), order.get().getVar(), namesArray);
        } else if (namesArray.length != 0) {
            traversal.select(namesArray[0], namesArray[0], namesArray);
        }

        return traversal;
    }

    /**
     * @return a stream of concept names mentioned in the query
     */
    public Stream<String> getConcepts() {
        return innerQueries.stream().flatMap(ConjunctionQuery::getConcepts);
    }

    public Stream<GraqlTraversal> allGraqlTraversals() {
        List<Set<List<Fragment>>> collect = innerQueries.stream().map(ConjunctionQuery::allFragmentOrders).collect(toList());
        Set<List<List<Fragment>>> lists = Sets.cartesianProduct(collect);
        return lists.stream().map(list -> GraqlTraversal.create(graph, Sets.newHashSet(list)));
    }
}

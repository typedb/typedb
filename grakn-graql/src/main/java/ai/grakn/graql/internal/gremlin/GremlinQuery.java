/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.gremlin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import ai.grakn.GraknGraph;
import ai.grakn.graql.admin.Conjunction;
import ai.grakn.graql.admin.PatternAdmin;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;
import ai.grakn.graql.internal.query.match.MatchOrder;
import ai.grakn.util.ErrorMessage;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.grakn.graql.internal.util.CommonUtil.toImmutableSet;
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

        innerQueries = patterns.stream().map(p -> new ConjunctionQuery(p)).collect(toList());
    }

    /**
     * @return a gremlin traversal to execute to find results
     */
    public GraphTraversal<Vertex, Map<String, Vertex>> getTraversal() {
        ImmutableSet<ImmutableList<Fragment>> fragments =
                innerQueries.stream().map(ConjunctionQuery::getSortedFragments).collect(toImmutableSet());

        GraqlTraversal graqlTraversal = GraqlTraversal.create(graph, fragments);

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
     * @return a stream of concept IDs mentioned in the query
     */
    public Stream<String> getConcepts() {
        return innerQueries.stream().flatMap(ConjunctionQuery::getConcepts);
    }

}

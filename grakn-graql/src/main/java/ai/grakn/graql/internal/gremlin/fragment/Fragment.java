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

package ai.grakn.graql.internal.gremlin.fragment;

import ai.grakn.GraknTx;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted.weighted;

/**
 * represents a graph traversal, with one start point and optionally an end point
 * <p>
 * A fragment is composed of four things:
 * <ul>
 * <li>A gremlin traversal function, that takes a gremlin traversal and appends some new gremlin steps</li>
 * <li>A starting variable name, where the gremlin traversal must start from</li>
 * <li>An optional ending variable name, if the gremlin traversal navigates to a new Graql variable</li>
 * <li>A priority, that describes how efficient this traversal is to help with ordering the traversals</li>
 * </ul>
 * <p>
 * Variable names refer to Graql variables. Some of these variable names may be randomly-generated UUIDs, such as for
 * castings.
 * <p>
 * A {@code Fragment} is usually contained in a {@code EquivalentFragmentSet}, which contains multiple fragments describing
 * the different directions the traversal can be followed in, with different starts and ends.
 * <p>
 * A gremlin traversal is created from a {@code Query} by appending together fragments in order of priority, one from
 * each {@code EquivalentFragmentSet} describing the {@code Query}.
 *
 * @author Felix Chapman
 */
public abstract class Fragment {

    // TODO: Find a better way to represent these values (either abstractly, or better estimates)

    private static final long NUM_INSTANCES_PER_TYPE = 100;
    private static final long NUM_SUBTYPES_PER_TYPE = 3;
    private static final long NUM_RELATIONS_PER_INSTANCE = 30;
    private static final long NUM_TYPES_PER_ROLE = 3;
    private static final long NUM_ROLES_PER_TYPE = 3;
    private static final long NUM_ROLE_PLAYERS_PER_RELATION = 2;
    private static final long NUM_ROLE_PLAYERS_PER_ROLE = 1;
    private static final long NUM_RESOURCES_PER_VALUE = 2;

    static final double COST_INSTANCES_PER_TYPE = Math.log1p(NUM_INSTANCES_PER_TYPE);
    static final double COST_SUBTYPES_PER_TYPE = Math.log1p(NUM_SUBTYPES_PER_TYPE);
    static final double COST_RELATIONS_PER_INSTANCE = Math.log1p(NUM_RELATIONS_PER_INSTANCE);
    static final double COST_TYPES_PER_ROLE = Math.log1p(NUM_TYPES_PER_ROLE);
    static final double COST_ROLES_PER_TYPE = Math.log1p(NUM_ROLES_PER_TYPE);
    static final double COST_ROLE_PLAYERS_PER_RELATION = Math.log1p(NUM_ROLE_PLAYERS_PER_RELATION);
    static final double COST_ROLE_PLAYERS_PER_ROLE = Math.log1p(NUM_ROLE_PLAYERS_PER_ROLE);

    static final double COST_SAME_AS_PREVIOUS = Math.log1p(1);

    static final double COST_NODE_INDEX = -Math.log(NUM_INSTANCES_PER_TYPE);
    static final double COST_NODE_INDEX_VALUE = -Math.log(NUM_INSTANCES_PER_TYPE / NUM_RESOURCES_PER_VALUE);

    static final double COST_NODE_NEQ = -Math.log(2D);
    static final double COST_NODE_DATA_TYPE = -Math.log(AttributeType.DataType.SUPPORTED_TYPES.size() / 2D);
    static final double COST_NODE_UNSPECIFIC_PREDICATE = -Math.log(2D);
    static final double COST_NODE_REGEX = -Math.log(2D);
    static final double COST_NODE_NOT_INTERNAL = -Math.log(1.1D);
    static final double COST_NODE_IS_ABSTRACT = -Math.log(1.1D);

    // By default we assume the latest shard is 25% full
    public static final double SHARD_LOAD_FACTOR = 0.25;

    private Optional<Double> accurateFragmentCost = Optional.empty();

    /*
     * This is the memoized result of {@link #vars()}
     */
    private @Nullable
    ImmutableSet<Var> vars = null;

    /**
     * @param transform map defining id transform var -> new id
     * @return transformed fragment with id predicates transformed according to the transform
     */
    public Fragment transform(Map<Var, ConceptId> transform) {
        return this;
    }

    /**
     * Get the corresponding property
     */
    public abstract @Nullable
    VarProperty varProperty();

    /**
     * @return the variable name that this fragment starts from in the query
     */
    public abstract Var start();

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    public @Nullable
    Var end() {
        return null;
    }

    ImmutableSet<Var> otherVars() {
        return ImmutableSet.of();
    }

    /**
     * @return the variable names that this fragment requires to have already been visited
     */
    public Set<Var> dependencies() {
        return ImmutableSet.of();
    }

    /**
     * Convert the fragment to a set of weighted edges for query planning
     *
     * @param nodes all nodes in the query
     * @param edges a mapping from edge(child, parent) to its corresponding fragment
     * @return a set of edges
     */
    public Set<Weighted<DirectedEdge<Node>>> directedEdges(Map<NodeId, Node> nodes,
                                                           Map<Node, Map<Node, Fragment>> edges) {
        return Collections.emptySet();
    }

    /**
     * @param traversal the traversal to extend with this Fragment
     * @param graph     the graph to execute the traversal on
     */
    public final GraphTraversal<Vertex, ? extends Element> applyTraversal(
            GraphTraversal<Vertex, ? extends Element> traversal, GraknTx graph,
            Collection<Var> vars, @Nullable Var currentVar) {
        if (currentVar != null) {
            if (!currentVar.equals(start())) {
                if (vars.contains(start())) {
                    // If the variable name has been visited but the traversal is not at that variable name, select it
                    traversal.select(start().name());
                } else {
                    // Restart traversal when fragments are disconnected
                    traversal.V().as(start().name());
                }
            }
        } else {
            // If the variable name has not been visited yet, remember it and use the 'as' step
            traversal.as(start().name());
        }

        vars.add(start());

        traversal = applyTraversalInner(traversal, graph, vars);

        Var end = end();
        if (end != null) {
            assignVar(traversal, end, vars);
        }

        vars.addAll(vars());

        return traversal;
    }

    static <T, U> GraphTraversal<T, U> assignVar(GraphTraversal<T, U> traversal, Var var, Collection<Var> vars) {
        if (!vars.contains(var)) {
            // This variable name has not been encountered before, remember it and use the 'as' step
            return traversal.as(var.name());
        } else {
            // This variable name has been encountered before, confirm it is the same
            return traversal.where(P.eq(var.name()));
        }
    }

    /**
     * @param traversal the traversal to extend with this Fragment
     * @param graph     the graph to execute the traversal on
     * @param vars
     */
    abstract GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, GraknTx graph, Collection<Var> vars);


    /**
     * The name of the fragment
     */
    public abstract String name();

    /**
     * A starting fragment is a fragment that can start a traversal.
     * If any other fragment is present that refers to the same variable, the starting fragment can be omitted.
     */
    public boolean isStartingFragment() {
        return false;
    }

    /**
     * Get the cost for executing the fragment.
     */
    public double fragmentCost() {
        return accurateFragmentCost.orElse(internalFragmentCost());
    }

    public void setAccurateFragmentCost(double fragmentCost) {
        accurateFragmentCost = Optional.of(fragmentCost);
    }

    public abstract double internalFragmentCost();

    /**
     * If a fragment has fixed cost, the traversal is done using index. This makes the fragment a good starting point.
     * A plan should always start with these fragments when possible.
     */
    public boolean hasFixedFragmentCost() {
        return false;
    }

    public Fragment getInverse() {
        return this;
    }

    public Optional<Long> getShardCount(GraknTx tx) {
        return Optional.empty();
    }

    /**
     * Indicates whether the fragment can be used on an {@link org.apache.tinkerpop.gremlin.structure.Edge} as well as
     * a {@link org.apache.tinkerpop.gremlin.structure.Vertex}.
     */
    public boolean canOperateOnEdges() {
        return false;
    }

    /**
     * Get all variables in the fragment including the start and end (if present)
     */
    public final Set<Var> vars() {
        if (vars == null) {
            ImmutableSet.Builder<Var> builder = ImmutableSet.<Var>builder().add(start());
            Var end = end();
            if (end != null) builder.add(end);
            builder.addAll(otherVars());
            vars = builder.build();
        }

        return vars;
    }

    @Override
    public final String toString() {
        return start() + name() + Optional.ofNullable(end()).map(Object::toString).orElse("");
    }

    final Set<Weighted<DirectedEdge<Node>>> directedEdges(NodeId.NodeType nodeType,
                                                          Map<NodeId, Node> nodes,
                                                          Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, start(), nodes);
        Node end = Node.addIfAbsent(NodeId.NodeType.VAR, end(), nodes);
        Node middle = Node.addIfAbsent(nodeType, Sets.newHashSet(start(), end()), nodes);
        middle.setInvalidStartingPoint();

        addEdgeToFragmentMapping(middle, start, edgeToFragment);
        return Sets.newHashSet(
                weighted(DirectedEdge.from(start).to(middle), -fragmentCost()),
                weighted(DirectedEdge.from(middle).to(end), 0));
    }

    final Set<Weighted<DirectedEdge<Node>>> directedEdges(Var edge,
                                                          Map<NodeId, Node> nodes,
                                                          Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, start(), nodes);
        Node end = Node.addIfAbsent(NodeId.NodeType.VAR, end(), nodes);
        Node middle = Node.addIfAbsent(NodeId.NodeType.VAR, edge, nodes);
        middle.setInvalidStartingPoint();

        addEdgeToFragmentMapping(middle, start, edgeToFragment);
        return Sets.newHashSet(
                weighted(DirectedEdge.from(start).to(middle), -fragmentCost()),
                weighted(DirectedEdge.from(middle).to(end), 0));
    }

    private void addEdgeToFragmentMapping(Node child, Node parent, Map<Node, Map<Node, Fragment>> edgeToFragment) {
        if (!edgeToFragment.containsKey(child)) {
            edgeToFragment.put(child, new HashMap<>());
        }
        edgeToFragment.get(child).put(parent, this);
    }
}

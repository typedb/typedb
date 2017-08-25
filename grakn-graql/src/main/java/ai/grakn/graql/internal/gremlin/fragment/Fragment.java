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
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;

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
    private static final long NUM_INSTANCES_PER_SCOPE = 100;
    private static final long NUM_SUBTYPES_PER_TYPE = 3;
    private static final long NUM_RELATIONS_PER_INSTANCE = 30;
    private static final long NUM_SCOPES_PER_INSTANCE = 3;
    private static final long NUM_TYPES_PER_ROLE = 3;
    private static final long NUM_ROLES_PER_TYPE = 3;
    private static final long NUM_ROLE_PLAYERS_PER_RELATION = 2;
    private static final long NUM_ROLE_PLAYERS_PER_ROLE = 1;
    private static final long NUM_RESOURCES_PER_VALUE = 2;

    static final double COST_INSTANCES_PER_TYPE = Math.log1p(NUM_INSTANCES_PER_TYPE);
    static final double COST_INSTANCES_PER_SCOPE = Math.log1p(NUM_INSTANCES_PER_SCOPE);
    static final double COST_SUBTYPES_PER_TYPE = Math.log1p(NUM_SUBTYPES_PER_TYPE);
    static final double COST_RELATIONS_PER_INSTANCE = Math.log1p(NUM_RELATIONS_PER_INSTANCE);
    static final double COST_SCOPES_PER_INSTANCE = Math.log1p(NUM_SCOPES_PER_INSTANCE);
    static final double COST_TYPES_PER_ROLE = Math.log1p(NUM_TYPES_PER_ROLE);
    static final double COST_ROLES_PER_TYPE = Math.log1p(NUM_ROLES_PER_TYPE);
    static final double COST_ROLE_PLAYERS_PER_RELATION = Math.log1p(NUM_ROLE_PLAYERS_PER_RELATION);
    static final double COST_ROLE_PLAYERS_PER_ROLE = Math.log1p(NUM_ROLE_PLAYERS_PER_ROLE);

    static final double COST_INDEX = 0.05D; // arbitrary small number
    static final double COST_RESOURCES_PER_VALUE = Math.log1p(COST_INDEX * NUM_RESOURCES_PER_VALUE);

    static final double COST_SAME_AS_PREVIOUS = Math.log1p(1);
    static final double COST_NEQ = Math.log1p(0.5);
    static final double COST_DATA_TYPE = Math.log1p(2D / AttributeType.DataType.SUPPORTED_TYPES.size());
    static final double COST_UNSPECIFIC_PREDICATE = Math.log1p(0.5);

    /**
     * Get the corresponding property
     */
    public abstract VarProperty getVarProperty();

    /**
     * @return the variable name that this fragment starts from in the query
     */
    public abstract Var getStart();

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    public Optional<Var> getEnd() {
        return Optional.empty();
    }

    ImmutableSet<Var> otherVarNames() {
        return ImmutableSet.of();
    }

    /**
     * @return the variable names that this fragment requires to have already been visited
     */
    public Set<Var> getDependencies() {
        return ImmutableSet.of();
    }

    /**
     * Get all variable names in the fragment - the start and end (if present)
     */
    public final Set<Var> getVariableNames() {
        ImmutableSet.Builder<Var> builder = ImmutableSet.<Var>builder().add(getStart());
        getEnd().ifPresent(builder::add);
        builder.addAll(otherVarNames());
        return builder.build();
    }

    /**
     * Convert the fragment to a set of weighted edges for query planning
     *
     * @param nodes all nodes in the query
     * @param edges a mapping from edge(child, parent) to its corresponding fragment
     * @return a set of edges
     */
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return Collections.emptySet();
    }

    @Override
    public String toString() {
        return getStart() + getName() + getEnd().map(Object::toString).orElse("");
    }

    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(NodeId.NodeType nodeType,
                                                              Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, getStart(), nodes);
        Node end = Node.addIfAbsent(NodeId.NodeType.VAR, getEnd().get(), nodes);
        Node middle = Node.addIfAbsent(nodeType, Sets.newHashSet(getStart(), getEnd().get()), nodes);
        middle.setInvalidStartingPoint();

        addEdgeToFragmentMapping(middle, start, edgeToFragment);
        return Sets.newHashSet(
                weighted(DirectedEdge.from(start).to(middle), -fragmentCost()),
                weighted(DirectedEdge.from(middle).to(end), 0));
    }

    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Var edge,
                                                              Map<NodeId, Node> nodes,
                                                              Map<Node, Map<Node, Fragment>> edgeToFragment) {

        Node start = Node.addIfAbsent(NodeId.NodeType.VAR, getStart(), nodes);
        Node end = Node.addIfAbsent(NodeId.NodeType.VAR, getEnd().get(), nodes);
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

    /**
     * @param traversal the traversal to extend with this Fragment
     * @param graph     the graph to execute the traversal on
     */
    public abstract GraphTraversal<Element, ? extends Element> applyTraversal(
            GraphTraversal<Element, ? extends Element> traversal, GraknTx graph);

    /**
     * The name of the fragment
     */
    public abstract String getName();

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
    public abstract double fragmentCost();

    /**
     * If a fragment has fixed cost, the traversal is done using index. This makes the fragment a good starting point.
     * A plan should always start with these fragments when possible.
     */
    public boolean hasFixedFragmentCost() {
        return false;
    }

    /**
     * Indicates whether the fragment can be used on an {@link org.apache.tinkerpop.gremlin.structure.Edge} as well as
     * a {@link org.apache.tinkerpop.gremlin.structure.Vertex}.
     */
    public boolean canOperateOnEdges() {
        return false;
    }
}

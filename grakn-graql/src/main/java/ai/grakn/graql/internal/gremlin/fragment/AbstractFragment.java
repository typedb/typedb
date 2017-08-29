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

import ai.grakn.concept.AttributeType;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.DirectedEdge;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.Node;
import ai.grakn.graql.internal.gremlin.spanningtree.graph.NodeId;
import ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.spanningtree.util.Weighted.weighted;
import static ai.grakn.util.CommonUtil.optionalToStream;

abstract class AbstractFragment implements Fragment {

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

    static final double COST_INDEX = 0.05D; // arbitrary small number
    static final double COST_RESOURCES_PER_VALUE = Math.log1p(COST_INDEX * NUM_RESOURCES_PER_VALUE);

    static final double COST_SAME_AS_PREVIOUS = Math.log1p(1);
    static final double COST_NEQ = Math.log1p(0.5);
    static final double COST_DATA_TYPE = Math.log1p(2D / AttributeType.DataType.SUPPORTED_TYPES.size());
    static final double COST_UNSPECIFIC_PREDICATE = Math.log1p(0.5);

    private final Var start;
    private final Optional<Var> end;
    private final ImmutableSet<Var> varNames;
    private EquivalentFragmentSet equivalentFragmentSet = null;

    private VarProperty varProperty; // For reasoner to map fragments to atoms

    AbstractFragment(VarProperty varProperty, Var start) {
        this.varProperty = varProperty;
        this.start = start;
        this.end = Optional.empty();
        this.varNames = ImmutableSet.of(start);
    }

    AbstractFragment(VarProperty varProperty, Var start, Var end, Var... others) {
        this.varProperty = varProperty;
        this.start = start;
        this.end = Optional.of(end);
        this.varNames = ImmutableSet.<Var>builder().add(start).add(end).add(others).build();
    }

    AbstractFragment(VarProperty varProperty, Var start, Var end, Var other, Var... others) {
        this.varProperty = varProperty;
        this.start = start;
        this.end = Optional.of(end);
        this.varNames = ImmutableSet.<Var>builder().add(start).add(end).add(other).add(others).build();
    }

    static Var[] optionalVarToArray(Optional<Var> var) {
        return optionalToStream(var).toArray(Var[]::new);
    }

    @Override
    public final EquivalentFragmentSet getEquivalentFragmentSet() {
        Preconditions.checkNotNull(equivalentFragmentSet, "Should not call getEquivalentFragmentSet before setEquivalentFragmentSet");
        return equivalentFragmentSet;
    }

    @Override
    public final void setEquivalentFragmentSet(EquivalentFragmentSet equivalentFragmentSet) {
        this.equivalentFragmentSet = equivalentFragmentSet;
    }

    @Override
    public final Var getStart() {
        return start;
    }

    @Override
    public final Optional<Var> getEnd() {
        return end;
    }

    @Override
    public Set<Var> getDependencies() {
        return ImmutableSet.of();
    }

    @Override
    public Set<Var> getVariableNames() {
        return varNames;
    }

    @Override
    public Set<Weighted<DirectedEdge<Node>>> getDirectedEdges(Map<NodeId, Node> Nodes,
                                                              Map<Node, Map<Node, Fragment>> edges) {
        return Collections.emptySet();
    }

    @Override
    public VarProperty getVarProperty() {
        return varProperty;
    }

    @Override
    public String toString() {
        return start + getName() + end.map(Object::toString).orElse("");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractFragment that = (AbstractFragment) o;

        if (start != null ? !start.equals(that.start) : that.start != null) return false;
        if (end != null ? !end.equals(that.end) : that.end != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = start != null ? start.hashCode() : 0;
        result = 31 * result + (end != null ? end.hashCode() : 0);
        return result;
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
}

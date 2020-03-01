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

package grakn.core.graql.planning.gremlin.fragment;

import com.google.common.collect.ImmutableSet;
import grakn.common.util.Pair;
import grakn.core.kb.concept.api.AttributeType;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.gremlin.Fragment;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.InstanceNode;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

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
 */
public abstract class FragmentImpl implements Fragment {

    // Default values used for estimate internal fragment cost
    private static final double NUM_INSTANCES_PER_TYPE = 100D;
    private static final double NUM_SUBTYPES_PER_TYPE = 1.5D;
    private static final double NUM_RELATIONS_PER_INSTANCE = 30D;
    private static final double NUM_TYPES_PER_ROLE = 3D;
    private static final double NUM_ROLES_PER_TYPE = 3D;
    private static final double NUM_ROLE_PLAYERS_PER_RELATION = 2D;
    private static final double NUM_ROLE_PLAYERS_PER_ROLE = 1D;
    private static final double NUM_RESOURCES_PER_VALUE = 2D;

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
    static final double COST_NODE_DATA_TYPE = -Math.log(AttributeType.DataType.values().size() / 2D);
    static final double COST_NODE_UNSPECIFIC_PREDICATE = -Math.log(2D);
    static final double COST_NODE_REGEX = -Math.log(2D);
    static final double COST_NODE_NOT_INTERNAL = -Math.log(1.1D);
    static final double COST_NODE_IS_ABSTRACT = -Math.log(1.1D);

    private Double accurateFragmentCost = null;


    @Nullable private ImmutableSet<Variable> vars = null;
    protected final VarProperty varProperty;
    protected final Variable start;

    FragmentImpl(@Nullable VarProperty varProperty, Variable start) {
        this.varProperty = varProperty;
        this.start = start;
    }


    /**
     * @param transform map defining id transform var -> new id
     * @return transformed fragment with id predicates transformed according to the transform
     */
    @Override
    public Fragment transform(Map<Variable, ConceptId> transform) {
        return this;
    }

    /**
     * Get the corresponding property
     */
    @Nullable
    public VarProperty varProperty() {
        return varProperty;
    }

    /**
     * @return the variable name that this fragment starts from in the query
     */
    public Variable start() {
        return start;
    }

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    @Override
    public @Nullable
    Variable end() {
        return null;
    }

    ImmutableSet<Variable> otherVars() {
        return ImmutableSet.of();
    }

    /**
     * @return the variable names that this fragment requires to have already been visited
     */
    @Override
    public Set<Variable> dependencies() {
        return ImmutableSet.of();
    }

    /**
     * When building the query plan spanning tree, every fragment has a start defined with a variable
     * Some fragments are actually edges in JanusGraph (such as isa, sub, etc.)
     * These require another variable for the end() variable, and to force the MST algorithm to
     * traverse these JanusGraph edges too, we insert a fake middle node representing the edge.
     * We default to an INSTANCE_NODE node type, which is the most general node
     *
     * @return
     */
    @Override
    public Set<Node> getNodes() {
        NodeId startNodeId = NodeId.of(NodeId.Type.VAR, start());
        return Collections.singleton(new InstanceNode(startNodeId));
    }

    /**
     * Some fragments represent Edges in JanusGraph. Similar to `getNodes()`, a fake node is added
     * in the middle for the query planning step. These middle nodes are connected with a directed edge uniquely to another node -
     * this directed edge is therefore the mapping from (node,node) directed edge to the Fragment. This is used to
     * convert the fake middle node back into a Fragment after query planning is complete.
     * For pairs of nodes we may have two edges (node1, node2) and (node2, node1). These stem from the two
     * fragments that are `Equivalent` in EquivalentFragmentSet - directionality is used to disambiguate which choice to use
     *
     * @param nodes
     * @return
     */
    @Override
    public Pair<Node, Node> getMiddleNodeDirectedEdge(Map<NodeId, Node> nodes) {
        return null;
    }

    /**
     * Convert the fragment to a set of weighted edges for query planning
     *
     * @param nodes all nodes in the query
     * @return a set of edges
     */
    @Override
    public Set<Weighted<DirectedEdge>> directedEdges(Map<NodeId, Node> nodes) {
        return Collections.emptySet();
    }


    /**
     * @param traversal the traversal to extend with this Fragment
     */
    @Override
    public final GraphTraversal<Vertex, ? extends Element> applyTraversal(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager,
            Collection<Variable> vars, Variable currentVar) {


        if (currentVar != null) {
            if (!currentVar.equals(start())) {
                if (vars.contains(start())) {
                    // If the variable name has been visited but the traversal is not at that variable name, select it
                    traversal.select(start().symbol());
                } else {
                    // Restart traversal when fragments are disconnected
                    traversal.V();
                    selectVariable(traversal);
                }
            }
        } else {
            // this is the very start of the traversal, record the step using `as` as we haven't visited the variable yet
            selectVariable(traversal);
        }

        vars.add(start());

        traversal = applyTraversalInner(traversal, conceptManager, vars);

        Variable end = end();
        if (end != null) {
            assignVar(traversal, end, vars);
        }

        vars.addAll(vars());

        return traversal;
    }

    @Override
    public GraphTraversal<Vertex, ? extends Element> selectVariable(GraphTraversal<Vertex, ? extends Element> traversal) {
        traversal.as(start().symbol());
        return traversal;
    }

    static <T, U> GraphTraversal<T, U> assignVar(GraphTraversal<T, U> traversal, Variable var, Collection<Variable> vars) {
        if (!vars.contains(var)) {
            // This variable name has not been encountered before, remember it and use the 'as' step
            return traversal.as(var.symbol());
        } else {
            // This variable name has been encountered before, confirm it is the same
            return traversal.where(P.eq(var.symbol()));
        }
    }

    /**
     * @param traversal the traversal to extend with this Fragment
     * @param conceptManager
     * @param vars
     */
    abstract GraphTraversal<Vertex, ? extends Element> applyTraversalInner(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager, Collection<Variable> vars);


    /**
     * A starting fragment is a fragment that can start a traversal.
     * If any other fragment is present that refers to the same variable, the starting fragment can be omitted.
     */
    @Override
    public boolean validStartIfDisconnected() {
        return false;
    }

    /**
     * Get the cost for executing the fragment.
     */
    @Override
    public double fragmentCost() {
        if (accurateFragmentCost != null) return accurateFragmentCost;

        return internalFragmentCost();
    }

    @Override
    public void setAccurateFragmentCost(double fragmentCost) {
        accurateFragmentCost = fragmentCost;
    }

    /**
     * Estimate the "cost" of a starting point for each type of fixed cost fragment
     * These are cost heuristic proxies using statistics
     *
     * @return
     * @param conceptManager
     * @param keyspaceStatistics
     */
    @Override
    public double estimatedCostAsStartingPoint(ConceptManager conceptManager, KeyspaceStatistics keyspaceStatistics) {
        throw new UnsupportedOperationException("Fragment of type " + this.getClass() + " is not a fixed cost starting point - no esimated cost as a starting point.");
    }

    /**
     * If a fragment has fixed cost, the traversal is done using index. This makes the fragment a good starting point.
     * A plan should always start with these fragments when possible.
     */
    @Override
    public boolean hasFixedFragmentCost() {
        return false;
    }

    @Override
    public Fragment getInverse() {
        return this;
    }

    /**
     * Get all variables in the fragment including the start and end (if present)
     * memoise results in `vars`
     */
    @Override
    public final Set<Variable> vars() {
        if (vars == null) {
            ImmutableSet.Builder<Variable> builder = ImmutableSet.<Variable>builder().add(start());
            Variable end = end();
            if (end != null) builder.add(end);
            builder.addAll(otherVars());
            vars = builder.build();
        }

        return vars;
    }

    @Override
    public final String toString() {
        String str = start().symbol() + name();
        if (end() != null) str += end().symbol();

        return str;
    }

}

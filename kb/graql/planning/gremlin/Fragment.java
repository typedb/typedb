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
 *
 */

package grakn.core.kb.graql.planning.gremlin;

import grakn.common.util.Pair;
import grakn.core.kb.concept.api.ConceptId;
import grakn.core.kb.concept.manager.ConceptManager;
import grakn.core.kb.graql.planning.spanningtree.graph.DirectedEdge;
import grakn.core.kb.graql.planning.spanningtree.graph.Node;
import grakn.core.kb.graql.planning.spanningtree.graph.NodeId;
import grakn.core.kb.graql.planning.spanningtree.util.Weighted;
import grakn.core.kb.keyspace.KeyspaceStatistics;
import graql.lang.property.VarProperty;
import graql.lang.statement.Variable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface Fragment {

    /**
     * @param transform map defining id transform var -> new id
     * @return transformed fragment with id predicates transformed according to the transform
     */
    Fragment transform(Map<Variable, ConceptId> transform);

    /**
     * Get the corresponding property
     */
    @Nullable
    VarProperty varProperty();

    /**
     * @return the variable name that this fragment starts from in the query
     */
    Variable start();

    /**
     * @return the variable name that this fragment ends at in the query, if this query has an end variable
     */
    @Nullable
    Variable end();

    /**
     * @return the variable names that this fragment requires to have already been visited
     */
    Set<Variable> dependencies();

    /**
     * When building the query plan spanning tree, every fragment has a start defined with a variable
     * Some fragments are actually edges in JanusGraph (such as isa, sub, etc.)
     * These require another variable for the end() variable, and to force the MST algorithm to
     * traverse these JanusGraph edges too, we insert a fake middle node representing the edge.
     * We default to an INSTANCE_NODE node type, which is the most general node
     *
     * @return
     */
    Set<Node> getNodes();

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
    Pair<Node, Node> getMiddleNodeDirectedEdge(Map<NodeId, Node> nodes);

    /**
     * Convert the fragment to a set of weighted edges for query planning
     *
     * @param nodes all nodes in the query
     * @return a set of edges
     */
    Set<Weighted<DirectedEdge>> directedEdges(Map<NodeId, Node> nodes);

    /**
     * @param traversal the traversal to extend with this Fragment
     */
    GraphTraversal<Vertex, ? extends Element> applyTraversal(
            GraphTraversal<Vertex, ? extends Element> traversal, ConceptManager conceptManager,
            Collection<Variable> vars, Variable currentVar);

    GraphTraversal<Vertex, ? extends Element> selectVariable(GraphTraversal<Vertex, ? extends Element> traversal);

    /**
     * The name of the fragment
     */
    String name();

    /**
     * A starting fragment is a fragment that can start a traversal.
     * If any other fragment is present that refers to the same variable, the starting fragment can be omitted.
     */
    boolean validStartIfDisconnected();

    /**
     * Get the cost for executing the fragment.
     */
    double fragmentCost();

    void setAccurateFragmentCost(double fragmentCost);

    double internalFragmentCost();

    /**
     * Estimate the "cost" of a starting point for each type of fixed cost fragment
     * These are cost heuristic proxies using statistics
     *
     * @return
     * @param conceptManager
     * @param keyspaceStatistics
     */
    double estimatedCostAsStartingPoint(ConceptManager conceptManager, KeyspaceStatistics keyspaceStatistics);

    /**
     * If a fragment has fixed cost, the traversal is done using index. This makes the fragment a good starting point.
     * A plan should always start with these fragments when possible.
     */
    boolean hasFixedFragmentCost();

    Fragment getInverse();

    /**
     * Get all variables in the fragment including the start and end (if present)
     */
    Set<Variable> vars();
}

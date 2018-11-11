/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
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

package grakn.core.graql.internal.gremlin.spanningtree.graph;

import grakn.core.graql.Var;

import java.util.Collections;
import java.util.Set;

/**
 * The unique id of a node.
 *
 * @author Jason Liu
 */
public class NodeId {

    /**
     * The type of a node.
     * If the node contains a var from the query, its type is VAR.
     * If the node is an edge from the query, its type is the type of the fragment.
     **/
    public enum NodeType {
        ISA, PLAYS, RELATES, SUB, VAR
    }

    private final NodeType nodeType;
    private final Set<Var> vars;

    public NodeId(NodeType nodeType, Set<Var> vars) {
        this.nodeType = nodeType;
        this.vars = vars;
    }

    public NodeId(NodeType nodeType, Var var) {
        this(nodeType, Collections.singleton(var));
    }

    @Override
    public int hashCode() {
        int result = nodeType == null ? 0 : nodeType.hashCode();
        result = 31 * result + (vars == null ? 0 : vars.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeId that = (NodeId) o;

        return (nodeType != null ? nodeType.equals(that.nodeType) : that.nodeType == null) &&
                (vars != null ? vars.equals(that.vars) : that.vars == null);
    }

    @Override
    public String toString() {
        return nodeType + vars.toString();
    }
}

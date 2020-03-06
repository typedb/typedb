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

package grakn.core.kb.graql.planning.spanningtree.graph;

import graql.lang.statement.Variable;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The unique id of a node.
 *
 */
public class NodeId {

    /**
     * The type of a node.
     * If the node contains a var from the query, its type is VAR.
     * If the node is an edge from the query, its type is the type of the fragment.
     **/
    public enum Type {
        ISA, PLAYS, RELATES, SUB, VAR
    }

    private final Type nodeIdType;
    private final Set<Variable> vars;

    private NodeId(Type nodeIdType, Set<Variable> vars) {
        this.nodeIdType = nodeIdType;
        this.vars = vars;
    }

    public static NodeId of(Type nodeIdType, Set<Variable> vars) {
        return new NodeId(nodeIdType, vars);
    }

    public static NodeId of(Type nodeIdType, Variable var) {
        return new NodeId(nodeIdType, Collections.singleton(var));
    }

    public Set<Variable> getVars() {
        return vars;
    }

    public Type nodeIdType() {
        return nodeIdType;
    }

    @Override
    public int hashCode() {
        int result = nodeIdType == null ? 0 : nodeIdType.hashCode();
        result = 31 * result + (vars == null ? 0 : vars.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeId that = (NodeId) o;

        return (nodeIdType != null ? nodeIdType.equals(that.nodeIdType) : that.nodeIdType == null) &&
                (vars != null ? vars.equals(that.vars) : that.vars == null);
    }

    @Override
    public String toString() {
        String varNames = vars.stream().map(var -> var.symbol()).collect(Collectors.joining(","));
        return nodeIdType + varNames;
    }
}

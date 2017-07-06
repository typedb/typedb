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

package ai.grakn.graql.internal.gremlin.spanningtree.graph;

import ai.grakn.graql.Var;
import ai.grakn.graql.internal.gremlin.fragment.Fragment;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An node in a directed graph.
 *
 * @author Jason Liu
 */
public class Node {
    private String name;
    private Optional<Var> var;
    private boolean isValidStartingPoint = true;

    private Set<Fragment> fragmentsWithoutDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependency = new HashSet<>();
    private Set<Fragment> fragmentsWithDependencyVisited = new HashSet<>();
    private Set<Fragment> dependants = new HashSet<>();

    private Node(String name) {
        this.name = name;
        this.var = Optional.empty();
    }

    private Node(Var var) {
        this.name = var.getValue();
        this.var = Optional.of(var);
    }

    public static Node addIfAbsent(Var var, Map<String, Node> nodes) {
        if (!nodes.containsKey(var.getValue())) {
            nodes.put(var.getValue(), new Node(var));
        }
        return nodes.get(var.getValue());
    }

    public static Node addIfAbsent(String name, Map<String, Node> nodes) {
        if (!nodes.containsKey(name)) {
            nodes.put(name, new Node(name));
        }
        return nodes.get(name);
    }

    public Optional<Var> getVar() {
        return var;
    }

    public String getName() {
        return name;
    }

    public Set<Fragment> getFragmentsWithoutDependency() {
        return fragmentsWithoutDependency;
    }

    public Set<Fragment> getFragmentsWithDependency() {
        return fragmentsWithDependency;
    }

    public Set<Fragment> getFragmentsWithDependencyVisited() {
        return fragmentsWithDependencyVisited;
    }

    public Set<Fragment> getDependants() {
        return dependants;
    }

    public boolean isValidStartingPoint() {
        return isValidStartingPoint;
    }

    public void setInvalidStartingPoint() {
        isValidStartingPoint = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;
        return name.equals(node.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        return name;
    }
}

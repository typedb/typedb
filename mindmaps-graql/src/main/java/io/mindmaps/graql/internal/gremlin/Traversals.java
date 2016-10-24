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

package io.mindmaps.graql.internal.gremlin;

import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.pattern.property.VarPropertyInternal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

import static io.mindmaps.util.Schema.EdgeLabel.SUB;

public class Traversals {

    private Traversals() {}

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Vertex, Vertex> outSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.out(SUB.getLabel())).emit()).unfold();
    }

    @SuppressWarnings("unchecked")
    public static GraphTraversal<Vertex, Vertex> inSubs(GraphTraversal<Vertex, Vertex> traversal) {
        return traversal.union(__.identity(), __.repeat(__.in(SUB.getLabel())).emit()).unfold();
    }

    static Stream<EquivalentFragmentSet> equivalentFragmentSets(VarAdmin var) {
        return var.getImplicitInnerVars().stream().flatMap(Traversals::equivalentFragmentSetsBase);
    }

    private static Stream<EquivalentFragmentSet> equivalentFragmentSetsBase(VarAdmin var) {
        ShortcutTraversal shortcutTraversal = new ShortcutTraversal();
        Collection<EquivalentFragmentSet> traversals = new HashSet<>();

        // If the user has provided a variable name, it can't be represented with a shortcut edge because it may be
        // referred to later.
        if (var.isUserDefinedName()) {
            shortcutTraversal.setInvalid();
        }

        String start = var.getName();

        var.getProperties().forEach(property -> {
            VarPropertyInternal propertyInternal = (VarPropertyInternal) property;
            propertyInternal.modifyShortcutTraversal(shortcutTraversal);
            Collection<EquivalentFragmentSet> newTraversals = propertyInternal.match(start);
            traversals.addAll(newTraversals);
        });

        if (shortcutTraversal.isValid()) {
            return Stream.of(shortcutTraversal.getEquivalentFragmentSet());
        } else {
            return traversals.stream();
        }
    }
}

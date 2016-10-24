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

import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;

/**
 * A collection of {@code EquivalentFragmentSet}s that describe a {@code Var}.
 * <p>
 * A {@code VarTraversals} is constructed from a {@code Var} and produces several {@code EquivalentFragmentSet}s that
 * are used by {@code GremlinQuery} to produce gremlin traversals.
 * <p>
 * If possible, the {@code VarTraversals} will be represented using a {@code ShortcutTraversal}.
 */
class VarTraversals {

    private final ShortcutTraversal shortcutTraversal = new ShortcutTraversal();
    private final Collection<EquivalentFragmentSet> traversals = new HashSet<>();
    private final Collection<VarTraversals> innerVarTraversals = new HashSet<>();

    /**
     * Create VarTraversals to represent a Var
     * @param var the variable that this VarTraversal will represent
     */
    VarTraversals(VarAdmin var) {

        // If the user has provided a variable name, it can't be represented with a shortcut edge because it may be
        // referred to later.
        if (var.isUserDefinedName()) {
            shortcutTraversal.setInvalid();
        }

        String start = var.getName();

        var.getProperties().forEach(property -> {
            VarPropertyInternal propertyInternal = (VarPropertyInternal) property;
            propertyInternal.modifyShortcutTraversal(shortcutTraversal);
            Collection<EquivalentFragmentSet> traversals = propertyInternal.match(start);
            this.traversals.addAll(traversals);
            property.getImplicitInnerVars().map(VarTraversals::new).forEach(innerVarTraversals::add);
        });
    }

    /**
     * @return a stream of traversals describing the variable
     */
    public Stream<EquivalentFragmentSet> getTraversals() {
        Stream<EquivalentFragmentSet> myPatterns;
        Stream<EquivalentFragmentSet> innerPatterns = innerVarTraversals.stream().flatMap(VarTraversals::getTraversals);

        if (shortcutTraversal.isValid()) {
            myPatterns = Stream.of(shortcutTraversal.getEquivalentFragmentSet());
        } else {
            myPatterns = traversals.stream();
        }

        return Stream.concat(myPatterns, innerPatterns);
    }
}

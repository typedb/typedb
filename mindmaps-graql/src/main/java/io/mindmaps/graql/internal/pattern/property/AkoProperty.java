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

package io.mindmaps.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.mindmaps.graql.admin.UniqueVarProperty;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;
import io.mindmaps.graql.internal.gremlin.Traversals;

import java.util.Collection;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_BOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNIQUE;

public class AkoProperty implements NamedProperty, UniqueVarProperty {

    private final VarAdmin superType;

    public AkoProperty(VarAdmin superType) {
        this.superType = superType;
    }

    public VarAdmin getSuperType() {
        return superType;
    }

    @Override
    public String getName() {
        return "ako";
    }

    @Override
    public String getProperty() {
        return superType.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        return Sets.newHashSet(new MultiTraversalImpl(
                new FragmentImpl(Traversals::outAkos, EDGE_UNIQUE, start, superType.getName()),
                new FragmentImpl(Traversals::inAkos, EDGE_BOUNDED, superType.getName(), start)
        ));
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet(superType);
    }
}

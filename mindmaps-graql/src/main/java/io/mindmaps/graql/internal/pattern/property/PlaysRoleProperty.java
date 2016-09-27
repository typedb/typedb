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
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;

import java.util.Collection;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.getEdgePriority;
import static io.mindmaps.graql.internal.gremlin.Traversals.inAkos;
import static io.mindmaps.graql.internal.gremlin.Traversals.outAkos;
import static io.mindmaps.util.Schema.EdgeLabel.PLAYS_ROLE;

public class PlaysRoleProperty implements NamedProperty {

    private final VarAdmin role;

    public PlaysRoleProperty(VarAdmin role) {
        this.role = role;
    }

    public VarAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "plays-role";
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        return Sets.newHashSet(new MultiTraversalImpl(
                new FragmentImpl(
                        t -> inAkos(outAkos(t).out(PLAYS_ROLE.getLabel())),
                        getEdgePriority(PLAYS_ROLE, true), start, role.getName()
                ),
                new FragmentImpl(
                        t -> inAkos(outAkos(t).in(PLAYS_ROLE.getLabel())),
                        getEdgePriority(PLAYS_ROLE, false), role.getName(), start
                )
        ));
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet(role);
    }
}

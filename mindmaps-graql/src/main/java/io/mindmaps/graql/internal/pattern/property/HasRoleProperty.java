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
import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;

import java.util.Collection;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.getEdgePriority;
import static io.mindmaps.graql.internal.pattern.property.VarProperties.failDelete;
import static io.mindmaps.util.Schema.EdgeLabel.HAS_ROLE;

public class HasRoleProperty implements NamedProperty {

    private final VarAdmin role;

    public HasRoleProperty(VarAdmin role) {
        this.role = role;
    }

    public VarAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "has-role";
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        return Sets.newHashSet(new MultiTraversalImpl(
                new FragmentImpl(t -> t.out(HAS_ROLE.getLabel()), getEdgePriority(HAS_ROLE, true), start, role.getName()),
                new FragmentImpl(t -> t.in(HAS_ROLE.getLabel()), getEdgePriority(HAS_ROLE, false), role.getName(), start)
        ));
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet(role);
    }

    @Override
    public void deleteProperty(MindmapsGraph graph, Concept concept) {
        String roleId = role.getId().orElseThrow(() -> failDelete(this));
        concept.asRelationType().deleteHasRole(graph.getRoleType(roleId));
    }
}

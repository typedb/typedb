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
import static io.mindmaps.util.Schema.EdgeLabel.HAS_SCOPE;

public class HasScopeProperty implements NamedProperty {

    private final VarAdmin scope;

    public HasScopeProperty(VarAdmin scope) {
        this.scope = scope;
    }

    public VarAdmin getScope() {
        return scope;
    }

    @Override
    public String getName() {
        return "has-scope";
    }

    @Override
    public String getProperty() {
        return scope.getPrintableName();
    }

    @Override
    public Collection<MultiTraversal> getMultiTraversals(String start) {
        return Sets.newHashSet(new MultiTraversalImpl(
                new FragmentImpl(t -> t.out(HAS_SCOPE.getLabel()), getEdgePriority(HAS_SCOPE, true), start, scope.getName()),
                new FragmentImpl(t -> t.in(HAS_SCOPE.getLabel()), getEdgePriority(HAS_SCOPE, false), scope.getName(), start)
        ));
    }

    @Override
    public Collection<VarAdmin> getInnerVars() {
        return Sets.newHashSet(scope);
    }

    @Override
    public void deleteProperty(MindmapsGraph graph, Concept concept) {
        String scopeId = scope.getId().orElseThrow(() -> failDelete(this));
        concept.asRelation().deleteScope(graph.getInstance(scopeId));
    }
}

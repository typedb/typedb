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
import io.mindmaps.concept.Instance;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.internal.gremlin.FragmentImpl;
import io.mindmaps.graql.internal.gremlin.MultiTraversal;
import io.mindmaps.graql.internal.gremlin.MultiTraversalImpl;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_BOUNDED;
import static io.mindmaps.graql.internal.gremlin.FragmentPriority.EDGE_UNBOUNDED;
import static io.mindmaps.util.Schema.EdgeLabel.HAS_SCOPE;

public class HasScopeProperty extends AbstractVarProperty implements NamedProperty {

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
                new FragmentImpl(t -> t.out(HAS_SCOPE.getLabel()), EDGE_BOUNDED, start, scope.getName()),
                new FragmentImpl(t -> t.in(HAS_SCOPE.getLabel()), EDGE_UNBOUNDED, scope.getName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(scope);
    }

    @Override
    public void insertProperty(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Instance scopeInstance = insertQueryExecutor.getConcept(scope).asInstance();
        concept.asRelation().scope(scopeInstance);
    }

    @Override
    public void deleteProperty(MindmapsGraph graph, Concept concept) {
        String scopeId = scope.getId().orElseThrow(() -> failDelete(this));
        concept.asRelation().deleteScope(graph.getInstance(scopeId));
    }
}

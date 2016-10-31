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

package io.grakn.graql.internal.pattern.property;

import com.google.common.collect.Sets;
import io.grakn.MindmapsGraph;
import io.grakn.concept.Concept;
import io.grakn.concept.RoleType;
import io.grakn.graql.admin.VarAdmin;
import io.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import io.grakn.graql.internal.gremlin.fragment.Fragments;
import io.grakn.graql.internal.query.InsertQueryExecutor;

import java.util.Collection;
import java.util.stream.Stream;

public class HasRoleProperty extends AbstractVarProperty implements NamedProperty {

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
    public Collection<EquivalentFragmentSet> match(String start) {
        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.outHasRole(start, role.getName()),
                Fragments.inHasRole(role.getName(), start)
        ));
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.of(role);
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(role);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        RoleType roleType = insertQueryExecutor.getConcept(role).asRoleType();
        concept.asRelationType().hasRole(roleType);
    }

    @Override
    public void delete(MindmapsGraph graph, Concept concept) {
        String roleId = role.getId().orElseThrow(() -> failDelete(this));
        concept.asRelationType().deleteHasRole(graph.getRoleType(roleId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasRoleProperty that = (HasRoleProperty) o;

        return role.equals(that.role);

    }

    @Override
    public int hashCode() {
        return role.hashCode();
    }
}

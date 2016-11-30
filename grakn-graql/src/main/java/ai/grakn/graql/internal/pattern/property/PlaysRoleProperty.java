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

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.fragment.Fragments;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import com.google.common.collect.Sets;
import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.RoleType;

import java.util.Collection;
import java.util.stream.Stream;

public class PlaysRoleProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin role;
    private final boolean required;

    public PlaysRoleProperty(VarAdmin role, boolean required) {
        this.role = role;
        this.required = required;
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
    public Collection<EquivalentFragmentSet> match(String start) {
        return Sets.newHashSet(EquivalentFragmentSet.create(
                Fragments.outPlaysRole(start, role.getVarName(), required),
                Fragments.inPlaysRole(role.getVarName(), start, required)
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
        concept.asType().playsRole(roleType);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        String roleId = role.getId().orElseThrow(() -> failDelete(this));
        concept.asType().deletePlaysRole(graph.getRoleType(roleId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlaysRoleProperty that = (PlaysRoleProperty) o;

        if (required != that.required) return false;
        return role.equals(that.role);

    }

    @Override
    public int hashCode() {
        int result = role.hashCode();
        result = 31 * result + (required ? 1 : 0);
        return result;
    }
}

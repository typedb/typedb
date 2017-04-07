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

import ai.grakn.GraknGraph;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.TypeLabel;
import ai.grakn.graql.Graql;
import ai.grakn.graql.VarName;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.TypeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static ai.grakn.graql.internal.reasoner.Utility.getIdPredicate;

/**
 * Represents the {@code relates} property on a {@link RelationType}.
 *
 * This property can be queried, inserted or deleted.
 *
 * This property relates a {@link RelationType} and a {@link RoleType}. It indicates that a {@link Relation} whose
 * type is this {@link RelationType} may have a role-player playing the given {@link RoleType}.
 *
 * @author Felix Chapman
 */
public class RelatesProperty extends AbstractVarProperty implements NamedProperty {

    private final VarAdmin role;

    public RelatesProperty(VarAdmin role) {
        this.role = role;
    }

    public VarAdmin getRole() {
        return role;
    }

    @Override
    public String getName() {
        return "relates";
    }

    @Override
    public String getProperty() {
        return role.getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(relates(start, role.getVarName()));
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
        concept.asRelationType().relates(roleType);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        TypeLabel roleLabel = role.getTypeLabel().orElseThrow(() -> failDelete(this));
        concept.asRelationType().deleteRelates(graph.getType(roleLabel));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelatesProperty that = (RelatesProperty) o;

        return role.equals(that.role);

    }

    @Override
    public int hashCode() {
        return role.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        VarName varName = var.getVarName();
        VarAdmin roleVar = this.getRole();
        VarName roleVariable = roleVar.getVarName();
        IdPredicate rolePredicate = getIdPredicate(roleVariable, roleVar, vars, parent);

        VarAdmin hrVar = Graql.var(varName).relates(Graql.var(roleVariable)).admin();
        return new TypeAtom(hrVar, rolePredicate, parent);
    }
}

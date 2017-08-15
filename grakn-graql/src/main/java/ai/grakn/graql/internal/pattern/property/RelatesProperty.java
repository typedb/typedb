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
import ai.grakn.concept.Label;
import ai.grakn.concept.Relation;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.type.RelatesAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.relates;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code relates} property on a {@link RelationType}.
 *
 * This property can be queried, inserted or deleted.
 *
 * This property relates a {@link RelationType} and a {@link Role}. It indicates that a {@link Relation} whose
 * type is this {@link RelationType} may have a role-player playing the given {@link Role}.
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class RelatesProperty extends AbstractVarProperty implements NamedProperty {

    public static RelatesProperty of(VarPatternAdmin role) {
        return new AutoValue_RelatesProperty(role);
    }

    abstract VarPatternAdmin role();

    @Override
    public String getName() {
        return "relates";
    }

    @Override
    public String getProperty() {
        return role().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(relates(this, start, role().var()));
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(role());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(role());
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        Role role = executor.get(this.role().var()).asRole();
        executor.get(var).asRelationType().relates(role);
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of(var, this.role().var());
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        Label roleLabel = role().getTypeLabel().orElseThrow(() -> GraqlQueryException.failDelete(this));
        concept.asRelationType().deleteRelates(graph.getOntologyConcept(roleLabel));
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.var().asUserDefined();
        VarPatternAdmin roleVar = this.role();
        Var roleVariable = roleVar.var().asUserDefined();
        IdPredicate rolePredicate = getIdPredicate(roleVariable, roleVar, vars, parent);

        VarPatternAdmin hrVar = varName.relates(roleVariable).admin();
        return new RelatesAtom(hrVar, roleVariable, rolePredicate, parent);
    }
}

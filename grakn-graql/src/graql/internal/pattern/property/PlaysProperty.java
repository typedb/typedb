/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.internal.pattern.property;

import grakn.core.concept.ConceptId;
import grakn.core.concept.Role;
import grakn.core.concept.Thing;
import grakn.core.concept.Type;
import grakn.core.exception.GraqlQueryException;
import grakn.core.graql.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.binary.PlaysAtom;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static grakn.core.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Reperesents the {@code plays} property on a {@link grakn.core.concept.Type}.
 *
 * This property relates a {@link grakn.core.concept.Type} and a {@link Role}. It indicates that an
 * {@link Thing} whose type is this {@link grakn.core.concept.Type} is permitted to be a role-player
 * playing the role of the given {@link Role}.
 *
 */
@AutoValue
public abstract class PlaysProperty extends AbstractVarProperty implements NamedProperty {

    public static final String NAME = "plays";

    public static PlaysProperty of(VarPatternAdmin role, boolean required) {
        return new AutoValue_PlaysProperty(role, required);
    }

    abstract VarPatternAdmin role();

    abstract boolean required();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return role().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.plays(this, start, role().var(), required()));
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
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Role role = executor.get(this.role().var()).asRole();
            executor.get(var).asType().plays(role);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, role().var()).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            Role role = executor.get(this.role().var()).asRole();

            if (!type.isDeleted() && !role.isDeleted()) {
                type.unplay(role);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var, role().var()).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.var().asUserDefined();
        VarPatternAdmin typeVar = this.role();
        Var typeVariable = typeVar.var().asUserDefined();
        IdPredicate predicate = getIdPredicate(typeVariable, typeVar, vars, parent);
        ConceptId predicateId = predicate == null? null : predicate.getPredicate();
        return PlaysAtom.create(varName, typeVariable, predicateId, parent);
    }
}

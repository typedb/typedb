/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

import ai.grakn.GraknTx;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.VarPattern;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.reasoner.atom.binary.IsaAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * @author Jason Liu
 */

abstract class AbstractIsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    public abstract VarPatternAdmin type();

    @Override
    public final String getProperty() {
        return type().getPrintableName();
    }

    @Override
    public final Stream<VarPatternAdmin> getTypes() {
        return Stream.of(type());
    }

    @Override
    public final Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(type());
    }

    @Override
    public final Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(this.type().var()).asType();
            executor.builder(var).isa(type);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(type().var()).produces(var).build());
    }

    @Override
    public final void checkValidProperty(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException {
        type().getTypeLabel().ifPresent(typeLabel -> {
            SchemaConcept theSchemaConcept = graph.getSchemaConcept(typeLabel);
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(typeLabel);
            }
        });
    }

    @Nullable
    @Override
    public final Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        //IsaProperty is unique within a var, so skip if this is a relation
        if (var.hasProperty(RelationshipProperty.class)) return null;

        Var varName = var.var().asUserDefined();
        VarPatternAdmin typePattern = this.type();
        Var typeVariable = typePattern.var();

        IdPredicate predicate = getIdPredicate(typeVariable, typePattern, vars, parent);
        ConceptId predicateId = predicate != null ? predicate.getPredicate() : null;

        //isa part
        VarPatternAdmin isaVar = varPatternForAtom(varName, typeVariable).admin();
        return IsaAtom.create(isaVar, typeVariable, predicateId, parent);
    }

    protected abstract VarPattern varPatternForAtom(Var varName, Var typeVariable);
}

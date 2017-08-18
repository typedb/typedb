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

import ai.grakn.GraknTx;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Thing;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.binary.type.ScopeAtom;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.hasScope;
import static ai.grakn.graql.internal.reasoner.utils.ReasonerUtils.getIdPredicate;

/**
 * Represents the {@code has-scope} property on a {@link Relationship}.
 *
 * This property can be queried, inserted or deleted.
 *
 * This property relates a {@link Relationship} and an {@link Thing}, where the instance behaves as the "scope".
 *
 * @author Felix Chapman
 */
@AutoValue
public abstract class HasScopeProperty extends AbstractVarProperty implements NamedProperty {

    public static HasScopeProperty of(VarPatternAdmin scope) {
        return new AutoValue_HasScopeProperty(scope);
    }

    abstract VarPatternAdmin scope();

    @Override
    public String getName() {
        return "has-scope";
    }

    @Override
    public String getProperty() {
        return scope().getPrintableName();
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(hasScope(this, start, scope().var()));
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(scope());
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        Thing scopeThing = executor.get(scope().var()).asThing();
        executor.get(var).asType().scope(scopeThing);
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of(var, scope().var());
    }

    @Override
    public void delete(GraknTx graph, Concept concept) {
        IdProperty scopeId =
                scope().getProperty(IdProperty.class).orElseThrow(() -> GraqlQueryException.failDelete(this));
        concept.asType().deleteScope(graph.getConcept(scopeId.id()));
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        Var varName = var.var().asUserDefined();
        VarPatternAdmin scopeVar = this.scope();
        Var scopeVariable = scopeVar.var().asUserDefined();
        IdPredicate predicate = getIdPredicate(scopeVariable, scopeVar, vars, parent);

        //isa part
        VarPatternAdmin scVar = varName.hasScope(scopeVariable).admin();
        return new ScopeAtom(scVar, scopeVariable, predicate, parent);
    }
}

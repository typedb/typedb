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
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
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

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.hasScope;
import static ai.grakn.graql.internal.reasoner.Utility.getIdPredicate;

/**
 * Represents the {@code has-scope} property on a {@link Relation}.
 *
 * This property can be queried, inserted or deleted.
 *
 * This property relates a {@link Relation} and an {@link Instance}, where the instance behaves as the "scope".
 *
 * @author Felix Chapman
 */
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
    public Collection<EquivalentFragmentSet> match(VarName start) {
        return ImmutableSet.of(hasScope(start, scope.getVarName()));
    }

    @Override
    public Stream<VarAdmin> getInnerVars() {
        return Stream.of(scope);
    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
        Instance scopeInstance = insertQueryExecutor.getConcept(scope).asInstance();
        concept.asType().scope(scopeInstance);
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        ConceptId scopeId = scope.getId().orElseThrow(() -> failDelete(this));
        concept.asType().deleteScope(graph.getConcept(scopeId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HasScopeProperty that = (HasScopeProperty) o;

        return scope.equals(that.scope);

    }

    @Override
    public int hashCode() {
        return scope.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarAdmin var, Set<VarAdmin> vars, ReasonerQuery parent) {
        VarName varName = var.getVarName();
        VarAdmin scopeVar = this.getScope();
        VarName scopeVariable = scopeVar.getVarName();
        IdPredicate predicate = getIdPredicate(scopeVariable, scopeVar, vars, parent);

        //isa part
        VarAdmin scVar = Graql.var(varName).hasScope(Graql.var(scopeVariable)).admin();
        return new TypeAtom(scVar, predicate, parent);
    }
}

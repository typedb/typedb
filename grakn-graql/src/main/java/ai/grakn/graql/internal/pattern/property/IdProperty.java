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

import ai.grakn.concept.ConceptId;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.predicate.IdPredicate;
import ai.grakn.graql.internal.util.StringConverter;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code id} property on a {@link ai.grakn.concept.Concept}.
 *
 * This property can be queried. While this property cannot be inserted, if used in an insert query any existing concept
 * with the given ID will be retrieved.
 *
 * @author Felix Chapman
 */
public class IdProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "id";

    private final ConceptId id;

    public IdProperty(ConceptId id) {
        this.id = id;
    }

    public ConceptId getId() {
        return id;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return StringConverter.idToString(id);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.id(this, start, id));
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        executor.builder(var).id(id);
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of();
    }

    @Override
    public Set<Var> producedVars(Var var) {
        return ImmutableSet.of(var);
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdProperty that = (IdProperty) o;

        return id.equals(that.id);

    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new IdPredicate(var.var(), this, parent);
    }
}

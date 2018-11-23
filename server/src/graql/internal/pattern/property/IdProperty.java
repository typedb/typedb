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

import grakn.core.graql.concept.ConceptId;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.query.Var;
import grakn.core.graql.admin.Atomic;
import grakn.core.graql.admin.ReasonerQuery;
import grakn.core.graql.admin.UniqueVarProperty;
import grakn.core.graql.admin.VarPatternAdmin;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets;
import grakn.core.graql.internal.reasoner.atom.predicate.IdPredicate;
import grakn.core.graql.internal.util.StringConverter;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Represents the {@code id} property on a {@link Concept}.
 *
 * This property can be queried. While this property cannot be inserted, if used in an insert query any existing concept
 * with the given ID will be retrieved.
 *
 */
@AutoValue
public abstract class IdProperty extends AbstractVarProperty implements NamedProperty, UniqueVarProperty {

    public static final String NAME = "id";

    public static IdProperty of(ConceptId id) {
        return new AutoValue_IdProperty(id);
    }

    public abstract ConceptId id();

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return StringConverter.idToString(id());
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(EquivalentFragmentSets.id(this, start, id()));
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            executor.builder(var).id(id());
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).produces(var).build());
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        // This property works in both insert and define queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        // This property works in undefine queries, because it is only for look-ups
        return insert(var);
    }

    @Override
    public boolean uniquelyIdentifiesConcept() {
        return true;
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return IdPredicate.create(var.var(), id(), parent);
    }
}

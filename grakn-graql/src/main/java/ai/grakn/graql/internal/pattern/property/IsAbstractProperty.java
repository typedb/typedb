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

import ai.grakn.concept.Concept;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.graql.internal.reasoner.atom.property.IsAbstractAtom;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

import static ai.grakn.graql.internal.gremlin.sets.EquivalentFragmentSets.isAbstract;

/**
 * Represents the {@code is-abstract} property on a {@link ai.grakn.concept.Type}.
 *
 * This property can be matched or inserted.
 *
 * This property states that a type cannot have direct instances.
 *
 * @author Felix Chapman
 */
public class IsAbstractProperty extends AbstractVarProperty implements UniqueVarProperty {

    private static final IsAbstractProperty INSTANCE = new IsAbstractProperty();

    private IsAbstractProperty() {

    }

    public static IsAbstractProperty get() {
        return INSTANCE;
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append("is-abstract");
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(isAbstract(this, start));
    }

    @Override
    public void insert(Var var, InsertQueryExecutor executor) throws GraqlQueryException {
        Concept concept = executor.get(var);
        if(concept.isType()){
            concept.asType().setAbstract(true);
        } else {
            throw GraqlQueryException.insertAbstractOnNonType(concept.asOntologyConcept());
        }
    }

    @Override
    public Set<Var> requiredVars(Var var) {
        return ImmutableSet.of(var);
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return new IsAbstractAtom(var.getVarName(), parent);
    }
}

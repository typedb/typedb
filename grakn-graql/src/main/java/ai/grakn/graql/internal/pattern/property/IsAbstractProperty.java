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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.pattern.property;

/*-
 * #%L
 * grakn-graql
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Concept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Atomic;
import ai.grakn.graql.admin.ReasonerQuery;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.graql.internal.gremlin.EquivalentFragmentSet;
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

    public static final String NAME = "is-abstract";

    private IsAbstractProperty() {

    }

    public static IsAbstractProperty get() {
        return INSTANCE;
    }

    @Override
    public void buildString(StringBuilder builder) {
        builder.append(NAME);
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Var start) {
        return ImmutableSet.of(isAbstract(this, start));
    }

    @Override
    String getName() {
        return NAME;
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Concept concept = executor.get(var);
            if (concept.isType()) {
                concept.asType().setAbstract(true);
            } else {
                throw GraqlQueryException.insertAbstractOnNonType(concept.asSchemaConcept());
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            if (!type.isDeleted()) {
                type.setAbstract(false);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }

    @Override
    public Atomic mapToAtom(VarPatternAdmin var, Set<VarPatternAdmin> vars, ReasonerQuery parent) {
        return IsAbstractAtom.create(var.var(), parent);
    }
}

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

package grakn.core.graql.query.pattern.property;

import com.google.common.collect.ImmutableSet;
import grakn.core.graql.concept.Concept;
import grakn.core.graql.concept.Type;
import grakn.core.graql.exception.GraqlQueryException;
import grakn.core.graql.internal.gremlin.EquivalentFragmentSet;
import grakn.core.graql.query.pattern.Variable;

import java.util.Collection;

import static grakn.core.graql.internal.gremlin.sets.EquivalentFragmentSets.isAbstract;

/**
 * Represents the {@code is-abstract} property on a Type.
 * This property can be matched or inserted.
 * This property states that a type cannot have direct instances.
 */
public class IsAbstractProperty extends VarProperty {

    private static final IsAbstractProperty INSTANCE = new IsAbstractProperty();

    public static final String NAME = "is-abstract";

    private IsAbstractProperty() {}

    public static IsAbstractProperty get() {
        return INSTANCE;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getProperty() {
        return null;
    }

    @Override
    public boolean isUnique() {
        return true;
    }

    @Override
    public String toString() {
        return NAME;
    }

    @Override
    public Collection<EquivalentFragmentSet> match(Variable start) {
        return ImmutableSet.of(isAbstract(this, start));
    }

    @Override
    public Collection<PropertyExecutor> define(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Concept concept = executor.get(var);
            if (concept.isType()) {
                concept.asType().isAbstract(true);
            } else {
                throw GraqlQueryException.insertAbstractOnNonType(concept.asSchemaConcept());
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Variable var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(var).asType();
            if (!type.isDeleted()) {
                type.isAbstract(false);
            }
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(var).build());
    }
}

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

import ai.grakn.GraknTx;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.UniqueVarProperty;
import ai.grakn.graql.admin.VarPatternAdmin;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * @author Jason Liu
 */

abstract class AbstractIsaProperty extends AbstractVarProperty implements UniqueVarProperty, NamedProperty {

    public abstract VarPatternAdmin type();

    @Override
    public String getProperty() {
        return type().getPrintableName();
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.of(type());
    }

    @Override
    public Stream<VarPatternAdmin> innerVarPatterns() {
        return Stream.of(type());
    }

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        PropertyExecutor.Method method = executor -> {
            Type type = executor.get(this.type().var()).asType();
            executor.builder(var).isa(type);
        };

        return ImmutableSet.of(PropertyExecutor.builder(method).requires(type().var()).produces(var).build());
    }

    @Override
    public void checkValidProperty(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException {
        type().getTypeLabel().ifPresent(typeLabel -> {
            SchemaConcept theSchemaConcept = graph.getSchemaConcept(typeLabel);
            if (theSchemaConcept != null && !theSchemaConcept.isType()) {
                throw GraqlQueryException.cannotGetInstancesOfNonType(typeLabel);
            }
        });
    }
}

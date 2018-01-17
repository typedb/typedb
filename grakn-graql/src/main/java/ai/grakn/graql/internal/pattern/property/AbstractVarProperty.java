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
import ai.grakn.exception.GraqlQueryException;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.VarPatternAdmin;
import ai.grakn.util.CommonUtil;

import java.util.Collection;
import java.util.stream.Stream;

abstract class AbstractVarProperty implements VarPropertyInternal {

    @Override
    public final void checkValid(GraknTx graph, VarPatternAdmin var) throws GraqlQueryException {
        checkValidProperty(graph, var);

        innerVarPatterns().map(VarPatternAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream).forEach(label -> {
            if (graph.getSchemaConcept(label) == null) {
                throw GraqlQueryException.labelNotFound(label);
            }
        });
    }

    void checkValidProperty(GraknTx graph, VarPatternAdmin var) {

    }

    abstract String getName();

    @Override
    public Collection<PropertyExecutor> insert(Var var) throws GraqlQueryException {
        throw GraqlQueryException.insertUnsupportedProperty(getName());
    }

    @Override
    public Collection<PropertyExecutor> define(Var var) throws GraqlQueryException {
        throw GraqlQueryException.defineUnsupportedProperty(getName());
    }

    @Override
    public Collection<PropertyExecutor> undefine(Var var) throws GraqlQueryException {
        throw GraqlQueryException.defineUnsupportedProperty(getName());
    }

    @Override
    public Stream<VarPatternAdmin> getTypes() {
        return Stream.empty();
    }

    @Override
    public Stream<VarPatternAdmin> implicitInnerVarPatterns() {
        return innerVarPatterns();
    }

    @Override
    public final String toString() {
        return graqlString();
    }
}

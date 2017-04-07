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
import ai.grakn.graql.internal.query.InsertQueryExecutor;
import ai.grakn.concept.Concept;
import ai.grakn.graql.admin.VarAdmin;
import ai.grakn.graql.admin.VarProperty;
import ai.grakn.graql.internal.util.CommonUtil;
import ai.grakn.util.ErrorMessage;

import java.util.stream.Stream;

abstract class AbstractVarProperty implements VarPropertyInternal {

    @Override
    public final void checkValid(GraknGraph graph, VarAdmin var) throws IllegalStateException {
        checkValidProperty(graph, var);

        getInnerVars().map(VarAdmin::getTypeLabel).flatMap(CommonUtil::optionalToStream).forEach(label -> {
            if (graph.getType(label) == null) {
                throw new IllegalStateException(ErrorMessage.LABEL_NOT_FOUND.getMessage(label));
            }
        });
    }

    void checkValidProperty(GraknGraph graph, VarAdmin var) {

    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
    }

    @Override
    public void delete(GraknGraph graph, Concept concept) {
        throw failDelete(this);
    }

    @Override
    public Stream<VarAdmin> getTypes() {
        return Stream.empty();
    }

    @Override
    public Stream<VarAdmin> getImplicitInnerVars() {
        return getInnerVars();
    }

    static IllegalStateException failDelete(VarProperty property) {
        StringBuilder builder = new StringBuilder();
        property.buildString(builder);
        return new IllegalStateException(ErrorMessage.DELETE_UNSUPPORTED_PROPERTY.getMessage(builder.toString()));
    }
}

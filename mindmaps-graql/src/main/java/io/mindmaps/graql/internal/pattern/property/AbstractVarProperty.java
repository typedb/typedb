/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.graql.internal.pattern.property;

import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.Concept;
import io.mindmaps.graql.admin.VarAdmin;
import io.mindmaps.graql.admin.VarProperty;
import io.mindmaps.graql.internal.query.InsertQueryExecutor;
import io.mindmaps.graql.internal.util.CommonUtil;
import io.mindmaps.util.ErrorMessage;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.stream.Stream;

abstract class AbstractVarProperty implements VarPropertyInternal {

    @Override
    public final int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public final boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public final void checkValid(MindmapsGraph graph, VarAdmin var) throws IllegalStateException {
        checkValidProperty(graph, var);

        getTypes().map(VarAdmin::getId).flatMap(CommonUtil::optionalToStream).forEach(typeId -> {
            if (graph.getConcept(typeId) == null) {
                throw new IllegalStateException(ErrorMessage.ID_NOT_FOUND.getMessage(typeId));
            }
        });
    }

    void checkValidProperty(MindmapsGraph graph, VarAdmin var) {

    }

    @Override
    public void insert(InsertQueryExecutor insertQueryExecutor, Concept concept) throws IllegalStateException {
    }

    @Override
    public void delete(MindmapsGraph graph, Concept concept) {
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

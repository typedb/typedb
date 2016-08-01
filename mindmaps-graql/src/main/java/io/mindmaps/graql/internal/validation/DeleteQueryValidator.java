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

package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.DeleteQuery;

import java.util.stream.Stream;

/**
 * A validator for a delete query that validates all deleters in the query
 */
public class DeleteQueryValidator implements Validator {

    private final DeleteQuery.Admin deleteQuery;

    /**
     * @param deleteQuery the delete query to validate
     */
    public DeleteQueryValidator(DeleteQuery.Admin deleteQuery) {
        this.deleteQuery = deleteQuery;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        Stream<Validator> validators = deleteQuery.getDeleters().stream().map(DeleteVarValidator::new);
        return Validator.getAggregateValidator(validators).getErrors(transaction);
    }
}

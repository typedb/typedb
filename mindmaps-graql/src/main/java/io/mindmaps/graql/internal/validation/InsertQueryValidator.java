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
import io.mindmaps.graql.api.query.InsertQuery;

import java.util.stream.Stream;

/**
 * A validator for an insert query, which validates the vars in the insert query
 */
public class InsertQueryValidator implements Validator {

    private final InsertQuery.Admin insertQuery;

    /**
     * @param insertQuery the insert query to validate
     */
    public InsertQueryValidator(InsertQuery.Admin insertQuery) {
        this.insertQuery = insertQuery;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        Stream<Validator> validators = insertQuery.getAllVars().stream().map(InsertVarValidator::new);
        return Validator.getAggregateValidator(validators).getErrors(transaction);
    }
}

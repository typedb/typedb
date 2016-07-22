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
import io.mindmaps.graql.api.query.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A validator for a Var in an insert query
 */
class InsertVarValidator implements Validator {

    private final Var.Admin var;
    private final List<String> errors = new ArrayList<>();

    /**
     * @param var the Var in an insert query to validate
     */
    public InsertVarValidator(Var.Admin var) {
        this.var = var;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        if (var.usesNonEqualPredicate()) {
            errors.add(ErrorMessage.INSERT_PREDICATE.getMessage());
        }

        if (var.isRelation() && !var.getType().isPresent()) {
            errors.add(ErrorMessage.INSERT_RELATION_WITHOUT_ISA.getMessage());
        }

        return errors.stream();
    }
}

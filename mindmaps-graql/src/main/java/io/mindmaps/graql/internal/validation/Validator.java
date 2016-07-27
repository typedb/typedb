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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * A Validator for validating something in a Graql query
 */
interface Validator {
    /**
     * @param patterns a stream of validators to combine into a single validator
     * @return a validator that executes all the given validators
     */
    static Validator getAggregateValidator(Stream<? extends Validator> patterns) {
        return new AggregateValidator(patterns.collect(toList()));
    }

    /**
     * @param transaction the transaction to use for validating a query
     * @return a stream of errors found during validation
     */
    Stream<String> getErrors(MindmapsTransaction transaction);

    /**
     * @param transaction the transaction to use for validating a query
     * @throws IllegalStateException when a problem was found in a query
     */
    default void validate(MindmapsTransaction transaction) throws IllegalStateException {
        List<String> errors = getErrors(transaction).collect(Collectors.toList());
        if (!errors.isEmpty()) {
            throw new IllegalStateException(String.join("\n", errors));
        }
    }
}

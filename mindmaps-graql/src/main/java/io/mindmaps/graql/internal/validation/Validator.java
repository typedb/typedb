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

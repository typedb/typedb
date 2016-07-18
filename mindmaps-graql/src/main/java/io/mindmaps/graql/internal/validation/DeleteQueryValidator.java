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

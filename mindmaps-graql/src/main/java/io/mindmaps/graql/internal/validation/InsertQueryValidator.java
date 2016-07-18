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

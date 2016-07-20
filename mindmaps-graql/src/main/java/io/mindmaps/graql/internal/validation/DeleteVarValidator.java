package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.Var;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * A validator that validates a deleter Var in a delete query
 */
class DeleteVarValidator implements Validator {

    private final Var.Admin var;

    /**
     * @param var a deleter Var in a delete query
     */
    public DeleteVarValidator(Var.Admin var) {
        this.var = var;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        List<String> errors = new ArrayList<>();

        if (var.hasValue()) errors.add(ErrorMessage.DELETE_VALUE.getMessage());

        return errors.stream();
    }
}

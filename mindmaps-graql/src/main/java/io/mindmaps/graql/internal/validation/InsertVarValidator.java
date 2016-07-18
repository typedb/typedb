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

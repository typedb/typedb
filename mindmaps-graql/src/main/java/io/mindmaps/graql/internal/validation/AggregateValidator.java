package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * An AggregateValidator that combines several Validators
 */
class AggregateValidator implements Validator {
    private final Collection<? extends Validator> validators;

    /**
     * @param validators the validators to combine into one validator
     */
    public AggregateValidator(Collection<? extends Validator> validators) {
        this.validators = validators;
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        return validators.stream().flatMap(v -> v.getErrors(transaction));
    }
}

package io.mindmaps.graql.internal.validation;

import io.mindmaps.core.dao.MindmapsTransaction;
import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A validator for a collection of vars
 */
public class PatternValidator implements Validator {

    private final Collection<Var.Admin> vars;

    /**
     * @param pattern A collection of vars
     */
    public PatternValidator(Pattern.Admin pattern) {
        this.vars = pattern.getVars();
    }

    @Override
    public Stream<String> getErrors(MindmapsTransaction transaction) {
        Stream<Validator> validators = vars.stream().map(MatchVarValidator::new);
        return Validator.getAggregateValidator(validators).getErrors(transaction);
    }
}

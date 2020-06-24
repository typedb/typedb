package grakn.core.test.behaviour.resolution.common;

import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;

import java.util.function.Function;

public class StatementVisitor extends PatternVisitor {

    private Function<Statement, Pattern> function;

    public StatementVisitor(Function<Statement, Pattern> function) {
        this.function = function;
    }

    @Override
    Pattern visitStatement(Statement pattern) {
        return function.apply(pattern);
    }
}

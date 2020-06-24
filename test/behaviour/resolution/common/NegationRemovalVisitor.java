package grakn.core.test.behaviour.resolution.common;

import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;

public class NegationRemovalVisitor extends PatternVisitor {
    @Override
    Pattern visitNegation(Negation<? extends Pattern> pattern) {
        return null;
    }
}

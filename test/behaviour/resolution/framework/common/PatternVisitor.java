package grakn.core.test.behaviour.resolution.framework.common;

import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Disjunction;
import graql.lang.pattern.Negation;
import graql.lang.pattern.Pattern;
import graql.lang.statement.Statement;

import java.util.HashSet;
import java.util.Set;

public abstract class PatternVisitor {
    public Pattern visitPattern(Pattern pattern) {
        if (pattern instanceof Statement) {
            return visitStatement((Statement) pattern);
        } else if (pattern instanceof Conjunction) {
            return visitConjunction((Conjunction<? extends Pattern>) pattern);
        } else if (pattern instanceof Negation) {
            return visitNegation((Negation<? extends Pattern>) pattern);
        } else if (pattern instanceof Disjunction) {
            return visitDisjunction((Disjunction<? extends Pattern>) pattern);
        }
        throw new UnsupportedOperationException();
    }

    Pattern visitStatement(Statement pattern) {
        return pattern;
    }

    Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Conjunction<>(newPatterns);
    }

    Pattern visitNegation(Negation<? extends Pattern> pattern) {
        return new Negation<>(visitPattern(pattern.getPattern()));
    }

    private Pattern visitDisjunction(Disjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern != null) {
                newPatterns.add(childPattern);
            }
        });
        return new Disjunction<>(newPatterns);
    }
}

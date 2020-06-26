package grakn.core.test.behaviour.resolution.framework.common;

import graql.lang.pattern.Conjunction;
import graql.lang.pattern.Pattern;

import java.util.HashSet;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class ConjunctionFlatteningVisitor extends PatternVisitor {
    Pattern visitConjunction(Conjunction<? extends Pattern> pattern) {
        Set<? extends Pattern> patterns = pattern.getPatterns();
        HashSet<Pattern> newPatterns = new HashSet<>();
        patterns.forEach(p -> {
            Pattern childPattern = visitPattern(p);
            if (childPattern instanceof Conjunction) {
                newPatterns.addAll(((Conjunction<? extends Pattern>) childPattern).getPatterns());
            } else if (childPattern != null) {
                newPatterns.add(visitPattern(childPattern));
            }
        });
        if (newPatterns.size() == 0) {
            return null;
        } else if (newPatterns.size() == 1) {
            return getOnlyElement(newPatterns);
        }
        return new Conjunction<>(newPatterns);
    }
}

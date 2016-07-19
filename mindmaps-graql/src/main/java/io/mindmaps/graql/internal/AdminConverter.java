package io.mindmaps.graql.internal;

import io.mindmaps.graql.api.query.Pattern;
import io.mindmaps.graql.api.query.Var;

import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Helper methods for converting classes to admin equivalents
 */
public class AdminConverter {

    /**
     * @param patterns a collection of patterns to change to admin
     * @return a collection of Pattern.Admin from the given patterns
     */
    public static Set<Pattern.Admin> getPatternAdmins(Collection<? extends Pattern> patterns) {
        return patterns.stream().map(Pattern::admin).collect(toSet());
    }

    /**
     * @param patterns a collection of vars to change to admin
     * @return a collection of Var.Admin from the given patterns
     */
    public static Set<Var.Admin> getVarAdmins(Collection<? extends Var> patterns) {
        return patterns.stream().map(Var::admin).collect(toSet());
    }
}

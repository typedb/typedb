/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graql.query.pattern;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import grakn.core.graql.concept.Label;
import grakn.core.graql.query.pattern.property.VarProperty;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.CheckReturnValue;


/**
 * Factory for instances of {@link Pattern}.
 */
public class Patterns {

    private static AtomicLong counter = new AtomicLong(System.currentTimeMillis() * 1000);

    /**
     * @param name the name of the variable
     * @return a new query variable
     */
    @CheckReturnValue
    public static Variable var(String name) {
        return new Variable(name, Variable.Type.UserDefined);
    }

    /**
     * @return a new, anonymous query variable
     */
    @CheckReturnValue
    public static Variable var() {
        return new Variable(Long.toString(counter.getAndIncrement()), Variable.Type.Generated);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static Statement label(String label) {
        return var().label(label);
    }

    /**
     * @param label the label of a concept
     * @return a variable pattern that identifies a concept by label
     */
    @CheckReturnValue
    public static Statement label(Label label) {
        return var().label(label);
    }

    /**
     * @param patterns an array of patterns to form a conjunction
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Pattern and(Pattern... patterns) {
        return and(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to form a conjunction
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static Pattern and(Collection<? extends Pattern> patterns) {
        return and(Sets.newHashSet(patterns));
    }

    /**
     *
     * @param patterns a set of patterns to form a conjunction
     * @param <T> conjunction inner pattern type
     * @return a pattern that will match only when all contained patterns match
     */
    @CheckReturnValue
    public static <T extends Pattern> Conjunction<T> and(Set<T> patterns) {
        return new Conjunction<>(patterns);
    }

    /**
     * @param patterns an array of patterns to form a disjunction
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    public static Pattern or(Pattern... patterns) {
        return or(Arrays.asList(patterns));
    }

    /**
     * @param patterns a collection of patterns to form a disjunction
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    public static Pattern or(Collection<? extends Pattern> patterns) {
        // Simplify representation when there is only one alternative
        if (patterns.size() == 1) {
            return Iterables.getOnlyElement(patterns);
        }
        return or(Sets.newHashSet(patterns));
    }

    /**
     *
     * @param patterns a set of patterns to form a disjunction
     * @param <T> disjunction inner pattern type
     * @return a pattern that will match when any contained pattern matches
     */
    @CheckReturnValue
    public static <T extends Pattern> Disjunction<T> or(Set<T> patterns) {
        return new Disjunction<>(patterns);
    }

    /**
     *
     * @param patterns an array of patterns to form a negation
     * @return a pattern that will match when no contained pattern matches
     */
    @CheckReturnValue
    public static Pattern not(Pattern... patterns) {
        return not(Sets.newHashSet(patterns));
    }

    /**
     * @param patterns a collection of patterns to form a negation
     * @return a pattern that will match when no contained pattern matches
     */
    @CheckReturnValue
    public static Pattern not(Collection<? extends Pattern> patterns) {
        return and(Sets.newHashSet(patterns));
    }

    /**
     *
     * @param patterns a set of patterns to form a negation
     * @param <T> negation inner pattern type
     * @return a pattern that will match when no contained pattern matches
     */
    @CheckReturnValue
    public static <T extends Pattern> Negation<T> not(Set<T> patterns) {
        return new Negation<>(patterns);
    }

    /**
     *
     * @param name statement variable name
     * @param properties statement consitutent properties
     * @param positive true if it is a positive statement
     * @return corresponding statement
     */
    @CheckReturnValue
    public static Statement statement(Variable name, Set<VarProperty> properties, boolean positive) {
        if (properties.isEmpty()) {
            return name;
        } else {
            return positive? new PositiveStatement(name, properties) : new NegativeStatement(name, properties);
        }
    }
}

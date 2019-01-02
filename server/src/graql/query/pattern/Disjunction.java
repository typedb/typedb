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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import grakn.core.graql.query.Query.Char;
import grakn.core.graql.query.Query.Operator;

import javax.annotation.CheckReturnValue;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A class representing a disjunction (or) of patterns. Any inner pattern must match in a query
 *
 * @param <T> the type of patterns in this disjunction
 */
public class Disjunction<T extends Pattern> implements Pattern {

    // TODO: Make Disjunction always store a set of Conjunction<?>, and thus removing <T>
    private final LinkedHashSet<T> patterns;

    Disjunction(Set<T> patterns) {
        if (patterns == null) {
            throw new NullPointerException("Null patterns");
        }
        this.patterns = patterns.stream()
                .map(Objects::requireNonNull)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * @return the patterns within this disjunction
     */
    @CheckReturnValue
    public Set<T> getPatterns() {
        return patterns;
    }

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        // Concatenate all disjunctions into one big disjunction
        Set<Conjunction<Statement>> dnf = getPatterns().stream()
                .flatMap(p -> p.getDisjunctiveNormalForm().getPatterns().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return Pattern.or(dnf);
    }

    @Override
    public Set<Variable> variables() {
        return getPatterns().stream().map(Pattern::variables).reduce(Sets::intersection).orElse(ImmutableSet.of());
    }

    @Override
    public boolean isDisjunction() {
        return true;
    }

    @Override
    public Disjunction<?> asDisjunction() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder disjunction = new StringBuilder();

        Iterator<T> patternIter = patterns.iterator();
        while (patternIter.hasNext()) {
            Pattern pattern = patternIter.next();
            disjunction.append(Char.CURLY_OPEN).append(Char.SPACE);

            if (pattern instanceof Conjunction<?>) {
                disjunction.append(((Conjunction<? extends Pattern>) pattern).getPatterns().stream()
                                           .map(Object::toString)
                                           .collect(Collectors.joining(Char.SPACE.toString())));
            } else {
                disjunction.append(pattern.toString());
            }

            disjunction.append(Char.SPACE).append(Char.CURLY_CLOSE);

            if (patternIter.hasNext()) {
                disjunction.append(Char.SPACE).append(Operator.OR).append(Char.SPACE);
            }
        }

        disjunction.append(Char.SEMICOLON);

        return disjunction.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o instanceof Disjunction) {
            Disjunction<?> that = (Disjunction<?>) o;
            return (this.patterns.equals(that.patterns));
        }
        return false;
    }

    @Override
    public int hashCode() {
        int h = 1;
        h *= 1000003;
        h ^= this.patterns.hashCode();
        return h;
    }
}

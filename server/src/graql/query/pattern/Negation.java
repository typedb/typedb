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

import javax.annotation.CheckReturnValue;
import java.util.Set;

/**
 * A class representing a negation of patterns. All inner patterns must not match in a query.
 *
 * @param <T> the type of patterns in this negation
 */
public class Negation<T extends Pattern> implements Pattern {

    private final T pattern;

    public Negation(T pattern) {
        if (pattern == null) {
            throw new NullPointerException("Null patterns");
        }
        this.pattern = pattern;
    }

    @CheckReturnValue
    public T getPattern(){ return pattern;}

    @Override
    public Disjunction<Conjunction<Statement>> getDisjunctiveNormalForm() {
        return pattern.negate().getDisjunctiveNormalForm();
    }

    @Override
    public Pattern negate() {
        return pattern;
    }

    @Override
    public Set<Variable> variables() {
        return pattern.variables();
    }

    @Override
    public String toString() {
        return "NOT {" + pattern + "; }";
    }
}


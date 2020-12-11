/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.logic;

import grakn.common.collection.Pair;
import grakn.core.logic.concludable.ConjunctionConcludable;
import grakn.core.logic.concludable.ThenConcludable;
import grakn.core.pattern.variable.Variable;

import java.util.Set;

public class Unification {

    private final ConjunctionConcludable<?, ?> fromConcludable;
    private final ThenConcludable<?, ?> toConcludable;
    private final Set<Pair<Variable, Variable>>  variableMapping;

    public Unification(ConjunctionConcludable<?, ?> fromConcludable, ThenConcludable<?, ?> toConcludable,
                       Set<Pair<Variable, Variable>> variableMapping) {
        this.fromConcludable = fromConcludable;
        this.toConcludable = toConcludable;
        this.variableMapping = variableMapping;
    }

    public ConjunctionConcludable<?, ?> fromConcludable() {
        return fromConcludable;
    }

    public ThenConcludable<?, ?> toConcludable() {
        return toConcludable;
    }

    // TODO personally I don't like this - there's no such thing as an empty Unified (compared to an empty unifier,
    //  which could be empty). Either a Unified form is found, or it isn't
    public static Unification EMPTY = new Empty();

    private static class Empty extends Unification {
        Empty() {
            super(null, null, null);
        }
    }
}

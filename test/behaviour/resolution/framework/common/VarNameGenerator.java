/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.test.behaviour.resolution.framework.common;

import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typeql.lang.pattern.variable.BoundVariable;
import com.vaticle.typeql.lang.pattern.variable.ThingVariable;

import java.util.HashMap;
import java.util.function.Function;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;
import static com.vaticle.typedb.core.test.behaviour.resolution.framework.common.VarNameGenerator.VarPrefix.*;


public class VarNameGenerator {

    private final HashMap<String, Integer> nextVarIndex = new HashMap<>();

    enum VarPrefix {
        X("x"), ANON("anon");

        private final String name;

        VarPrefix(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }
    /**
     * Creates a new variable by incrementing a value
     *
     * @param prefix The prefix to use to uniquely identify a set of incremented variables, e.g. `x` will give
     *               `x0`, `x1`, `x2`...
     * @return prefix followed by an auto-incremented integer, as a string
     */
    public String getNextVarName(VarPrefix prefix) {
        nextVarIndex.putIfAbsent(prefix.toString(), 0);
        int currentIndex = nextVarIndex.get(prefix.toString());
        String nextVar = prefix.toString() + currentIndex;
        nextVarIndex.put(prefix.toString(), currentIndex + 1);
        return nextVar;
    }

    public Function<BoundVariable, BoundVariable> deanonymiseIfAnon() {
        return this::deanonymiseIfAnon;
    }

    public BoundVariable deanonymiseIfAnon(BoundVariable variable) {
        if (variable.isThing()) return deanonymiseIfAnon(variable.asThing());
        else throw TypeDBException.of(ILLEGAL_STATE); // TODO: Check this is illegal
    }

    private ThingVariable<?> deanonymiseIfAnon(ThingVariable<?> variable) {
        if (variable.isNamed()) {
            return variable;
        } else {
            return variable.deanonymise(getNextVarName(ANON));
        }
    }
}

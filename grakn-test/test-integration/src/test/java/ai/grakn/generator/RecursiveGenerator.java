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

package ai.grakn.generator;

import com.pholser.junit.quickcheck.generator.GenerationStatus;

/**
 * Generator for recursive value types. Designed to stop after a certain depth.
 *
 * <p>
 *     A simple example of a recursive value type is an arithmetic expression. You could define it like this:
 * </p>
 *
 * <p>
 *     "An expression is either a number, an expression plus an expression, or an expression times an expression."
 * </p>
 *
 * <p>
 *     This lets you define expressions like: {@code (1 + 5) * 3}. Notice an expression may contain other expressions.
 *     When generating such an expression, the result could be enormous, or never terminate. This abstract class makes
 *     sure that the generated object only recurses up to a certain depth.
 * </p>
 *
 * @author Felix Chapman
 */
public abstract class RecursiveGenerator<T> extends AbstractGenerator<T> {

    RecursiveGenerator(Class<T> type) {
        super(type);
    }

    private static final GenerationStatus.Key<Integer> DEPTH = new GenerationStatus.Key<>("depth", Integer.class);
    private static final int MAX_DEPTH = 3;

    @Override
    protected final T generate() {
        int depth = status.valueOf(DEPTH).orElse(MAX_DEPTH);

        status.setValue(DEPTH, depth - 1);

        T result;

        if (depth > 0) {
            result = generateRecurse();
        } else {
            result = generateBase();
        }

        status.setValue(DEPTH, depth);

        return result;
    }

    /**
     * Generate a "base case" value. This method should <i>not</i> generate any recursive type.
     */
    protected abstract T generateBase();

    /**
     * Generate a "recursive" value. This method is permitted to recursively generate more values of the same type.
     */
    protected abstract T generateRecurse();
}
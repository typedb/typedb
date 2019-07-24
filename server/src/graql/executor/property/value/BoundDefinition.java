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

package grakn.core.graql.executor.property.value;

import org.apache.commons.lang.StringUtils;

public abstract class BoundDefinition<U> {

    abstract U lowerBound();
    abstract U upperBound();
    abstract boolean lowerBoundTest(U a1, U a2);
    abstract boolean upperBoundTest(U a1, U a2);


    static class NumberBound extends BoundDefinition<Number>{
        @Override
        Number lowerBound() {
            return Double.NEGATIVE_INFINITY;
        }

        @Override
        Number upperBound() {
            return Double.POSITIVE_INFINITY;
        }

        @Override
        boolean lowerBoundTest(Number thisBound, Number thatBound) {
            return thisBound.doubleValue() >= thatBound.doubleValue();
        }

        @Override
        boolean upperBoundTest(Number thisBound, Number thatBound) {
            return thisBound.doubleValue() <= thatBound.doubleValue();
        }
    }

    static class LongBound extends BoundDefinition<Long>{
        @Override
        Long lowerBound() {
            return Long.MIN_VALUE;
        }

        @Override
        Long upperBound() {
            return Long.MAX_VALUE;
        }

        @Override
        boolean lowerBoundTest(Long thisBound, Long thatBound) {
            return thisBound >= thatBound;
        }

        @Override
        boolean upperBoundTest(Long thisBound, Long thatBound) {
            return thisBound <= thatBound;
        }
    }

    static class StringBound extends BoundDefinition<String>{
        @Override
        String lowerBound() {
            return "";
        }

        @Override
        String upperBound() {
            //TODO we bound it artificially and it's not very nice
            return StringUtils.repeat(String.valueOf(Character.MAX_VALUE), 24);
        }

        @Override
        boolean lowerBoundTest(String thisBound, String thatBound) {
            return thisBound.compareTo(thatBound) >= 0;
        }

        @Override
        boolean upperBoundTest(String thisBound, String thatBound) {
            return thisBound.compareTo(thatBound) <= 0;
        }
    }

    static class BooleanBound extends BoundDefinition<Boolean>{
        @Override
        Boolean lowerBound() {
            return false;
        }

        @Override
        Boolean upperBound() {
            return true;
        }

        @Override
        boolean lowerBoundTest(Boolean thisBound, Boolean thatBound) {
            return thisBound || !thatBound;
        }

        @Override
        boolean upperBoundTest(Boolean thisBound, Boolean thatBound) {
            return thatBound || !thisBound;
        }
    }

    static class VariableBound extends BoundDefinition<String>{
        @Override
        String lowerBound() {
            return null;
        }

        @Override
        String upperBound() {
            return null;
        }

        @Override
        boolean lowerBoundTest(String thisBound, String thatBound) {
            return true;
        }

        @Override
        boolean upperBoundTest(String thisBound, String thatBound) {
            return true;
        }
    }
}

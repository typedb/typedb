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
 *
 */

package grakn.core.concept.answer;

import grakn.core.common.exception.GraknException;

import javax.annotation.Nullable;

import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;
import static grakn.core.common.exception.ErrorMessage.Internal.ILLEGAL_STATE;

/**
 * A type of Answer object that contains a Number. Will either be a long or a double
 */
public class Numeric implements Answer {

    @Nullable
    private final Long longValue;
    @Nullable
    private final Double doubleValue;

    private Numeric(@Nullable Long longValue, @Nullable Double doubleValue) {
        this.longValue = longValue;
        this.doubleValue = doubleValue;
    }

    public static Numeric ofLong(long value) {
        return new Numeric(value, null);
    }

    public static Numeric ofDouble(double value) {
        return new Numeric(null, value);
    }

    public static Numeric ofNaN() {
        return new Numeric(null, null);
    }

    public boolean isLong() {
        return longValue != null;
    }

    public boolean isDouble() {
        return doubleValue != null;
    }

    public boolean isNaN() {
        return !isLong() && !isDouble();
    }

    public long asLong() {
        if (isLong()) return longValue;
        else throw GraknException.of(ILLEGAL_OPERATION);
    }

    public Double asDouble() {
        if (isDouble()) return doubleValue;
        else throw GraknException.of(ILLEGAL_OPERATION);
    }

    public Number asNumber() {
        if (isLong()) return longValue;
        else if (isDouble()) return doubleValue;
        else throw GraknException.of(ILLEGAL_STATE);
    }
}

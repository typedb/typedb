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

package grakn.core.traversal;

import grakn.common.collection.Pair;
import graql.lang.common.GraqlToken;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static grakn.common.collection.Collections.pair;

public class TraversalParameters {

    private final Map<Identifier, byte[]> iid;
    private final Map<Pair<Identifier, GraqlToken.Comparator>, LinkedList<Value>> values;

    public TraversalParameters() {
        iid = new HashMap<>();
        values = new HashMap<>();
    }

    public void putIID(Identifier.Variable identifier, byte[] iid) {
        this.iid.put(identifier, iid);
    }

    public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, boolean value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, long value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, double value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, String value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier.Variable identifier, GraqlToken.Comparator comparator, LocalDateTime value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public byte[] getIID(Identifier.Variable identifier) {
        return iid.get(identifier);
    }

    public Value popValue(Identifier.Variable identifier, GraqlToken.Comparator comparator) {
        return values.get(pair(identifier, comparator)).removeFirst();
    }

    static class Value {

        final Boolean booleanValue;
        final Long longValue;
        final Double doubleValue;
        final String stringValue;
        final LocalDateTime dateTimeValue;

        Value(boolean value) {
            booleanValue = value;
            longValue = null;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = null;
        }

        Value(long value) {
            booleanValue = null;
            longValue = value;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = null;
        }

        Value(double value) {
            booleanValue = null;
            longValue = null;
            doubleValue = value;
            stringValue = null;
            dateTimeValue = null;
        }

        Value(String value) {
            booleanValue = null;
            longValue = null;
            doubleValue = null;
            stringValue = value;
            dateTimeValue = null;
        }

        Value(LocalDateTime value) {
            booleanValue = null;
            longValue = null;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = value;
        }

        boolean isBoolean() { return booleanValue != null; }

        boolean isLong() { return longValue != null; }

        boolean isDouble() { return doubleValue != null; }

        boolean isString() { return stringValue != null; }

        boolean isDateTime() { return dateTimeValue != null; }

        Boolean getBoolean() { return booleanValue; }

        Long getLong() { return longValue; }

        Double getDouble() { return doubleValue; }

        LocalDateTime getDateTime() { return dateTimeValue; }
    }
}

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

    private final Map<Identifier, LinkedList<byte[]>> iids;
    private final Map<Pair<Identifier, GraqlToken.Comparator>, LinkedList<Value>> values;

    public TraversalParameters() {
        iids = new HashMap<>();
        values = new HashMap<>();
    }

    public void pushIID(Identifier identifier, byte[] iid) {
        iids.computeIfAbsent(identifier, r -> new LinkedList<>()).addLast(iid);
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, boolean value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, int value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, double value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, String value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, LocalDateTime value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public void pushValue(Identifier identifier, GraqlToken.Comparator comparator, Identifier value) {
        values.computeIfAbsent(pair(identifier, comparator), k -> new LinkedList<>()).addLast(new Value(value));
    }

    public byte[] popIID(Identifier identifier) {
        return iids.get(identifier).removeFirst();
    }

    public Value popValue(Identifier identifier, GraqlToken.Comparator comparator) {
        return values.get(pair(identifier, comparator)).removeFirst();
    }

    static class Value {

        final Boolean booleanValue;
        final Integer integerValue;
        final Double doubleValue;
        final String stringValue;
        final LocalDateTime dateTimeValue;
        final Identifier variableValue;

        Value(boolean value) {
            booleanValue = value;
            integerValue = null;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = null;
            variableValue = null;
        }

        Value(int value) {
            booleanValue = null;
            integerValue = value;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = null;
            variableValue = null;
        }

        Value(double value) {
            booleanValue = null;
            integerValue = null;
            doubleValue = value;
            stringValue = null;
            dateTimeValue = null;
            variableValue = null;
        }

        Value(String value) {
            booleanValue = null;
            integerValue = null;
            doubleValue = null;
            stringValue = value;
            dateTimeValue = null;
            variableValue = null;
        }

        Value(LocalDateTime value) {
            booleanValue = null;
            integerValue = null;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = value;
            variableValue = null;
        }

        Value(Identifier value) {
            booleanValue = null;
            integerValue = null;
            doubleValue = null;
            stringValue = null;
            dateTimeValue = null;
            variableValue = value;
        }

        boolean isBoolean() { return booleanValue != null; }

        boolean isInteger() { return integerValue != null; }

        boolean isDouble() { return doubleValue != null; }

        boolean isString() { return stringValue != null; }

        boolean isDateTime() { return dateTimeValue != null; }

        boolean isVariable() { return variableValue != null; }

        Boolean getBoolean() { return booleanValue; }

        Integer getInteger() { return integerValue; }

        Double getDouble() { return doubleValue; }

        LocalDateTime getDateTime() { return dateTimeValue; }

        Identifier getVariable() { return variableValue; }
    }
}

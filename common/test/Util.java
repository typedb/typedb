/*
 * Copyright (C) 2022 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.common.test;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.vaticle.typedb.core.common.exception.TypeDBException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Util {

    public static void assertThrowsTypeDBException(Runnable function, String errorCode) {
        try {
            function.run();
            fail();
        } catch (TypeDBException e) {
            assertEquals(errorCode, e.errorMessage().code());
        } catch (Exception e) {
            fail();
        }
    }

    public static void assertThrows(Runnable function) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assertTrue(true);
        }
    }

    public static <T extends RuntimeException> void assertThrows(Runnable function, Class<T> exceptionType) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assertTrue(exceptionType.isInstance(e));
        }
    }

    public static void assertNotThrows(Runnable function) {
        try {
            function.run();
        } catch (Exception e) {
            // fail but we want to see the exception
            throw e;
        }
    }

    public static void assertThrowsWithMessage(Runnable function, String message) {
        try {
            function.run();
            fail();
        } catch (RuntimeException e) {
            assert e.getMessage().contains(message);
        }
    }

    public static boolean jsonEquals(JsonValue first, JsonValue second, boolean orderedArrays) {
        if (first == second) return true;
        else if (!first.getClass().equals(second.getClass())) return false;
        else if (first.isObject()) return jsonObjectEquals(first.asObject(), second.asObject(), orderedArrays);
        else if (first.isArray()) return jsonArrayEquals(orderedArrays, first.asArray(), second.asArray());
        else return first.equals(second);
    }

    private static boolean jsonObjectEquals(JsonObject firstObject, JsonObject secondObject, boolean orderedArrays) {
        for (String firstKey : firstObject.names()) {
            JsonValue firstValue = firstObject.get(firstKey);
            JsonValue secondValue;
            if ((secondValue = secondObject.get(firstKey)) == null ||
                    !com.vaticle.typedb.core.common.test.Util.jsonEquals(firstValue, secondValue, orderedArrays)) {
                return false;
            }
        }
        return true;
    }

    private static boolean jsonArrayEquals(boolean orderedArrays, JsonArray firstArray, JsonArray secondArray) {
        if (firstArray.size() != secondArray.size()) return false;
        if (orderedArrays) return jsonArrayEqualsOrdered(firstArray, secondArray);
        else return jsonArrayEqualsUnordered(firstArray, secondArray);
    }

    private static boolean jsonArrayEqualsUnordered(JsonArray firstArray, JsonArray secondArray) {
        List<JsonValue> secondCopy = new ArrayList<>(secondArray.values());
        for (int i = 0; i < firstArray.size(); i++) {
            boolean found = false;
            int j;
            JsonValue firstValue = firstArray.get(i);
            for (j = 0; j < secondCopy.size(); j++) {
                if (com.vaticle.typedb.core.common.test.Util.jsonEquals(firstValue, secondCopy.get(j), false)) {
                    secondCopy.remove(j);
                    found = true;
                    break;
                }
            }
            if (!found) return false;
        }
        return true;
    }

    private static boolean jsonArrayEqualsOrdered(JsonArray firstArray, JsonArray secondArray) {
        for (int i = 0; i < firstArray.size(); i++) {
            if (!com.vaticle.typedb.core.common.test.Util.jsonEquals(firstArray.get(i), secondArray.get(i), true)) {
                return false;
            }
        }
        return true;
    }
}

/*
 * Copyright (C) 2023 Vaticle
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

package com.vaticle.typedb.core.test.behaviour.util;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.vaticle.typedb.common.collection.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Util {

    public static boolean jsonEquals(JsonValue first, JsonValue second, boolean orderedArrays) {
        if (first == second) return true;
        else if (!first.getClass().equals(second.getClass())) return false;
        else if (first.isObject()) {
            JsonObject firstObject = first.asObject();
            JsonObject secondObject = second.asObject();
            for (String firstKey : firstObject.names()) {
                JsonValue firstValue = firstObject.get(firstKey);
                JsonValue secondValue;
                if ((secondValue = secondObject.get(firstKey)) == null ||
                        !jsonEquals(firstValue, secondValue, orderedArrays)) {
                    return false;
                }
            }
            return true;
        } else if (first.isArray()) {
            JsonArray firstArray = first.asArray();
            JsonArray secondArray = second.asArray();
            if (firstArray.size() != secondArray.size()) return false;

            if (orderedArrays) {
                for (int i = 0; i < firstArray.size(); i++) {
                    if (!jsonEquals(firstArray.get(i), secondArray.get(i), orderedArrays)) {
                        return false;
                    }
                }
                return true;
            } else {
                List<JsonValue> secondCopy = new ArrayList<>(secondArray.values());
                for (int i = 0; i < firstArray.size(); i++) {
                    boolean found = false;
                    int j;
                    JsonValue firstValue = firstArray.get(i);
                    for (j = 0; j < secondCopy.size(); j++) {
                        if (jsonEquals(firstValue, secondCopy.get(j), orderedArrays)) {
                            secondCopy.remove(j);
                            found = true;
                            break;
                        }
                    }
                    if (!found) return false;
                }
                return true;
            }
        } else {
            return first.equals(second);
        }
    }

    private static Set<Pair<String, JsonValue>> toPairs(JsonObject jsonObject) {
        Set<Pair<String, JsonValue>> pairs = new HashSet<>();
        for (String name : jsonObject.names()) {
            pairs.add(new Pair<>(name, jsonObject.get(name)));
        }
        return pairs;
    }
}

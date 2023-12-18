/*
 * Copyright (C) 2022 Vaticle
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

package com.vaticle.typedb.core.test.runner;

public class TypeDBSingleton {

    private static TypeDBRunner typeDBRunner;

    public static void setTypeDBRunner(TypeDBRunner instance) {
        typeDBRunner = instance;
    }

    public static void resetTypeDBRunner() {
        if (typeDBRunner != null) {
            typeDBRunner.reset();
        }
    }

    public static void deleteTypeDBRunner() {
        if (typeDBRunner != null) {
            typeDBRunner.deleteFiles();
            typeDBRunner = null;
        }
    }

    public static TypeDBRunner getTypeDBRunner() {
        return typeDBRunner;
    }
}

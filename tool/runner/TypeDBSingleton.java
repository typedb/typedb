/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.tool.runner;

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

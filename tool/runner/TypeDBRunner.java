/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.tool.runner;

public interface TypeDBRunner {

    void start();

    String address();

    boolean isStopped();

    void stop();

    void deleteFiles();

    void reset();
}

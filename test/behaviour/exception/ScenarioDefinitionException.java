/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.exception;

public class ScenarioDefinitionException extends RuntimeException {
    public ScenarioDefinitionException(String message) {
        super(message);
    }
}

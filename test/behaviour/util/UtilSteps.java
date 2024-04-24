/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.util;

import io.cucumber.java.en.When;

public class UtilSteps {
    @When("set time-zone is: {word}")
    public void set_timezone(String value){
        System.setProperty("user.timezone", value);
    }
}


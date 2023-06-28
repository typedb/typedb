package com.vaticle.typedb.core.test.behaviour.Util;

import io.cucumber.java.en.When;

public class UtilSteps {
    @When("set time-zone is: {word}")
    public void set_timezone(String value){
        System.setProperty("user.timezone", value);
    }
}
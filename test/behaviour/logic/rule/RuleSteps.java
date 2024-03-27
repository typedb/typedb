/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.test.behaviour.logic.rule;

import com.vaticle.typedb.core.logic.Rule;
import io.cucumber.java.en.Then;

import java.util.List;
import java.util.Set;

import static com.vaticle.typedb.core.test.behaviour.connection.ConnectionSteps.tx;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RuleSteps {
    @Then("rules contain: {type_label}")
    public void rules_contain_label(String ruleLabel) {
        assertTrue(tx().logic().rules().anyMatch(rule -> rule.getLabel().equals(ruleLabel)));
    }

    @Then("rules do not contain: {type_label}")
    public void rules_do_not_contain_label(String ruleLabel) {
        assertTrue(tx().logic().rules().noneMatch(rule -> rule.getLabel().equals(ruleLabel)));
    }

    @Then("rules contain")
    public void rules_contain(List<String> ruleLabels) {
        Set<String> actuals = tx().logic().rules().map(Rule::getLabel).toSet();
        for (String ruleLabel : ruleLabels) {
            assertTrue(actuals.contains(ruleLabel));
        }
    }

    @Then("rules do not contain")
    public void rules_do_not_contain(List<String> ruleLabels) {
        Set<String> actuals = tx().logic().rules().map(Rule::getLabel).toSet();
        for (String ruleLabel : ruleLabels) {
            assertFalse(actuals.contains(ruleLabel));
        }
    }
}


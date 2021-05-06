/*
 * Copyright (C) 2021 Vaticle
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


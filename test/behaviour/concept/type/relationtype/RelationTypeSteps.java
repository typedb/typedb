/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.test.behaviour.concept.type.relationtype;

import grakn.core.common.parameters.Label;
import grakn.core.concept.type.RoleType;
import grakn.core.concept.type.Type;
import grakn.core.test.behaviour.config.Parameters;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static grakn.core.test.behaviour.connection.ConnectionSteps.tx;
import static grakn.core.test.behaviour.util.Util.assertThrows;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Behaviour Steps specific to RelationTypes
 */
public class RelationTypeSteps {

    @When("relation\\( ?{type_label} ?) set relates role: {type_label}")
    public void relation_type_set_relates_role(String relationLabel, String roleLabel) {
        tx().concepts().getRelationType(relationLabel).setRelates(roleLabel);
    }

    @When("relation\\( ?{type_label} ?) set relates role: {type_label}; throws exception")
    public void thing_set_relates_role_throws_exception(String relationLabel, String roleLabel) {
        assertThrows(() -> relation_type_set_relates_role(relationLabel, roleLabel));
    }

    @When("relation\\( ?{type_label} ?) set relates role: {type_label} as {type_label}")
    public void relation_type_set_relates_role_type_as(String relationLabel, String roleLabel, String superRole) {
        tx().concepts().getRelationType(relationLabel).setRelates(roleLabel, superRole);
    }

    @When("relation\\( ?{type_label} ?) set relates role: {type_label} as {type_label}; throws exception")
    public void thing_set_relates_role_type_as_throws_exception(String relationLabel, String roleLabel, String superRole) {
        assertThrows(() -> relation_type_set_relates_role_type_as(relationLabel, roleLabel, superRole));
    }

    @When("relation\\( ?{type_label} ?) unset related role: {type_label}")
    public void relation_type_unset_related_role(String relationLabel, String roleLabel) {
        tx().concepts().getRelationType(relationLabel).unsetRelates(roleLabel);
    }

    @When("relation\\( ?{type_label} ?) unset related role: {type_label}; throws exception")
    public void relation_type_unset_related_role_throws_exception(String relationLabel, String roleLabel) {
        assertThrows(() -> relation_type_unset_related_role(relationLabel, roleLabel));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) is null: {bool}")
    public void relation_type_get_role_type_is_null(String relationLabel, String roleLabel, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getRelationType(relationLabel).getRelates(roleLabel)));
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) set label: {type_label}")
    public void relation_type_get_role_type_set_label(String relationLabel, String roleLabel, String newLabel) {
        tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).setLabel(newLabel);
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get label: {type_label}")
    public void relation_type_get_role_type_get_label(String relationLabel, String roleLabel, String getLabel) {
        assertEquals(getLabel, tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).getLabel());
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) is abstract: {bool}")
    public void relation_type_get_role_type_is_abstract(String relationLabel, String roleLabel, boolean isAbstract) {
        assertEquals(isAbstract, tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).isAbstract());
    }

    private Set<Parameters.ScopedLabel> relation_type_get_related_roles_actuals(String relationLabel) {
        return tx().concepts().getRelationType(relationLabel).getRelates()
                .map(role -> new Parameters.ScopedLabel(role.getLabel())).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get related roles contain:")
    public void relation_type_get_related_roles_contain(String relationLabel, List<Parameters.ScopedLabel> roleLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_related_roles_actuals(relationLabel);
        assertTrue(actuals.containsAll(roleLabels));
    }

    @Then("relation\\( ?{type_label} ?) get related roles do not contain:")
    public void relation_type_get_related_roles_do_not_contain(String relationLabel, List<Parameters.ScopedLabel> roleLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_related_roles_actuals(relationLabel);
        for (Parameters.ScopedLabel roleLabel : roleLabels) {
            assertFalse(actuals.contains(roleLabel));
        }
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertype: {scoped_label}")
    public void relation_type_get_role_type_get_supertype(String relationLabel, String roleLabel, Parameters.ScopedLabel superLabel) {
        final RoleType superType = tx().concepts().getRelationType(superLabel.scope()).getRelates(superLabel.label());
        assertEquals(superType, tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).getSupertype());
    }

    private Set<Parameters.ScopedLabel> relation_type_get_role_type_supertypes_actuals(String relationLabel, String roleLabel) {
        return tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).getSupertypes()
                .map(role -> new Parameters.ScopedLabel(role.getLabel())).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertypes contain:")
    public void relation_type_get_role_type_get_supertypes_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> superLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_role_type_supertypes_actuals(relationLabel, roleLabel);
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertypes do not contain:")
    public void relation_type_get_role_type_get_supertypes_do_not_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> superLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_role_type_supertypes_actuals(relationLabel, roleLabel);
        for (Parameters.ScopedLabel superLabel : superLabels) {
            assertFalse(actuals.contains(superLabel));
        }
    }

    private Set<String> relation_type_get_role_type_players_actuals(String relationLabel, String roleLabel) {
        return tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).getPlayers().map(Type::getLabel).map(Label::toString).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get players contain:")
    public void relation_type_get_role_type_get_players_contain(String relationLabel, String roleLabel, List<String> playerLabels) {
        final Set<String> actuals = relation_type_get_role_type_players_actuals(relationLabel, roleLabel);
        assertTrue(actuals.containsAll(playerLabels));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get players do not contain:")
    public void relation_type_get_role_type_get_plays_do_not_contain(String relationLabel, String roleLabel, List<String> playerLabels) {
        final Set<String> actuals = relation_type_get_role_type_players_actuals(relationLabel, roleLabel);
        for (String superLabel : playerLabels) {
            assertFalse(actuals.contains(superLabel));
        }
    }

    private Set<Parameters.ScopedLabel> relation_type_get_role_type_subtypes_actuals(String relationLabel, String roleLabel) {
        return tx().concepts().getRelationType(relationLabel).getRelates(roleLabel).getSubtypes()
                .map(role -> new Parameters.ScopedLabel(role.getLabel())).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get subtypes contain:")
    public void relation_type_get_role_type_get_subtypes_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> subLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_role_type_subtypes_actuals(relationLabel, roleLabel);
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get subtypes do not contain:")
    public void relation_type_get_role_type_get_subtypes_do_not_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> subLabels) {
        final Set<Parameters.ScopedLabel> actuals = relation_type_get_role_type_subtypes_actuals(relationLabel, roleLabel);
        System.out.println(actuals);
        for (Parameters.ScopedLabel subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }
}

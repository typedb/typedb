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

package hypergraph.test.behaviour.concept.type.relationtype;

import hypergraph.common.exception.HypergraphException;
import hypergraph.concept.type.RoleType;
import hypergraph.test.behaviour.config.Parameters;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behaviour Steps specific to RelationTypes
 */
public class RelationTypeSteps {

    @When("relation\\( ?{type_label} ?) set relates role: {type_label}")
    public void relation_set_relates_role(String relationLabel, String roleLabel) {
        tx().concepts().getRelationType(relationLabel).relates(roleLabel);
    }

    @When("relation\\( ?{type_label} ?) fails at setting relates role: {type_label}")
    public void thing_fails_at_setting_relates_role(String relationLabel, String roleLabel) {
        try {
            tx().concepts().getRelationType(relationLabel).relates(roleLabel);
            fail();
        } catch (HypergraphException ignore) {
            assertTrue(true);
        }
    }

    @When("relation\\( ?{type_label} ?) set relates role: {type_label} as {type_label}")
    public void relation_set_relates_role_as(String relationLabel, String roleLabel, String superRole) {
        tx().concepts().getRelationType(relationLabel).relates(roleLabel, superRole);
    }

    @When("relation\\( ?{type_label} ?) fails at setting relates role: {type_label} as {type_label}")
    public void thing_fails_at_setting_relates_role_as(String relationLabel, String roleLabel, String superRole) {
        try {
            tx().concepts().getRelationType(relationLabel).relates(roleLabel, superRole);
            fail();
        } catch (HypergraphException ignore) {
            assertTrue(true);
        }
    }

    @When("relation\\( ?{type_label} ?) remove related role: {type_label}")
    public void relation_remove_related_role(String relationLabel, String roleLabel) {
        tx().concepts().getRelationType(relationLabel).unrelate(roleLabel);
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) is null: {bool}")
    public void relation_get_role_is_null(String relationLabel, String roleLabel, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getRelationType(relationLabel).role(roleLabel)));
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) set label: {type_label}")
    public void relation_get_role_set_label(String relationLabel, String roleLabel, String newLabel) {
        tx().concepts().getRelationType(relationLabel).role(roleLabel).label(newLabel);
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get label: {type_label}")
    public void relation_get_role_get_label(String relationLabel, String roleLabel, String getLabel) {
        assertEquals(getLabel, tx().concepts().getRelationType(relationLabel).role(roleLabel).label());
    }

    @When("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) is abstract: {bool}")
    public void relation_get_role_is_abstract(String relationLabel, String roleLabel, boolean isAbstract) {
        assertEquals(isAbstract, tx().concepts().getRelationType(relationLabel).role(roleLabel).isAbstract());
    }

    private Set<Parameters.ScopedLabel> relation_get_related_roles_actuals(String relationLabel) {
        return tx().concepts().getRelationType(relationLabel).roles()
                .map(role -> new Parameters.ScopedLabel(role.scopedLabel().split(":")[0],
                                                        role.scopedLabel().split(":")[1])).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get related roles contain:")
    public void relation_get_related_roles_contain(String relationLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_related_roles_actuals(relationLabel);
        assertTrue(actuals.containsAll(roleLabels));
    }

    @Then("relation\\( ?{type_label} ?) get related roles do not contain:")
    public void relation_get_related_roles_do_not_contain(String relationLabel, List<Parameters.ScopedLabel> roleLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_related_roles_actuals(relationLabel);
        for (Parameters.ScopedLabel roleLabel : roleLabels) {
            assertFalse(actuals.contains(roleLabel));
        }
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertype: {scoped_label}")
    public void relation_get_role_get_supertype(String relationLabel, String roleLabel, Parameters.ScopedLabel superLabel) {
        RoleType superType = tx().concepts().getRelationType(superLabel.scope()).role(superLabel.role());
        assertEquals(superType, tx().concepts().getRelationType(relationLabel).role(roleLabel).sup());
    }

    private Set<Parameters.ScopedLabel> relation_get_role_supertypes_actuals(String relationLabel, String roleLabel) {
        return tx().concepts().getRelationType(relationLabel).role(roleLabel).sups()
                .map(role -> new Parameters.ScopedLabel(role.scopedLabel().split(":")[0],
                                                        role.scopedLabel().split(":")[1])).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertypes contain:")
    public void relation_get_role_get_supertypes_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> superLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_role_supertypes_actuals(relationLabel, roleLabel);
        assertTrue(actuals.containsAll(superLabels));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertypes do not contain:")
    public void relation_get_role_get_supertypes_do_not_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> superLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_role_supertypes_actuals(relationLabel, roleLabel);
        for (Parameters.ScopedLabel superLabel : superLabels) {
            assertFalse(actuals.contains(superLabel));
        }
    }

    private Set<Parameters.ScopedLabel> relation_get_role_subtypes_actuals(String relationLabel, String roleLabel) {
        return tx().concepts().getRelationType(relationLabel).role(roleLabel).subs()
                .map(role -> new Parameters.ScopedLabel(role.scopedLabel().split(":")[0],
                                                        role.scopedLabel().split(":")[1])).collect(toSet());
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get subtypes contain:")
    public void relation_get_role_get_subtypes_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> subLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_role_subtypes_actuals(relationLabel, roleLabel);
        assertTrue(actuals.containsAll(subLabels));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get subtypes do not contain:")
    public void relation_get_role_get_subtypes_do_not_contain(String relationLabel, String roleLabel, List<Parameters.ScopedLabel> subLabels) {
        Set<Parameters.ScopedLabel> actuals = relation_get_role_subtypes_actuals(relationLabel, roleLabel);
        System.out.println(actuals);
        for (Parameters.ScopedLabel subLabel : subLabels) {
            assertFalse(actuals.contains(subLabel));
        }
    }

    @Then("relation\\( ?{type_label} ?) fails at creating an instance")
    public void relation_fails_at_creating_an_instance(String typeLabel) {
        try {
            tx().concepts().getRelationType(typeLabel).create();
            fail();
        } catch (Exception ignore) {
            assertTrue(true);
        }
    }
}

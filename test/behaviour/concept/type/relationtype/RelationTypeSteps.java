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

import hypergraph.concept.type.RoleType;
import hypergraph.concept.type.Type;
import hypergraph.test.behaviour.config.Parameters;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.util.List;
import java.util.Set;

import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Behaviour Steps specific to RelationTypes
 */
public class RelationTypeSteps {

    @When("relation\\( ?{type_label} ?) set relates role: {type_label}")
    public void relation_set_relates_role(String relationLabel, String roleLabel) {
        tx().concepts().putRelationType(relationLabel).relates(roleLabel);
    }

    @When("relation\\( ?{type_label} ?) set relates role: {type_label} as {type_label}")
    public void relation_set_relates_role_as(String relationLabel, String roleLabel, String superRole) {
        tx().concepts().putRelationType(relationLabel).relates(roleLabel).as(superRole);
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) is null: {bool}")
    public void relation_get_role_is_null(String relationLabel, String roleLabel, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().putRelationType(relationLabel).role(roleLabel)));
    }

    @Then("relation\\( ?{type_label} ?) get role\\( ?{type_label} ?) get supertype: {scoped_label}")
    public void relation_get_role_is_null(String relationLabel, String roleLabel, Parameters.ScopedLabel superLabel) {
        RoleType superType = tx().concepts().getRelationType(superLabel.scope()).role(superLabel.role());
        assertEquals(superType, tx().concepts().getRelationType(relationLabel).role(roleLabel).sup());
    }

    @Then("relation\\( ?{type_label} ?) get related roles contain:")
    public void thing_get_supertypes_contain(String relationLabel, List<String> roleLabels) {
        Set<String> actuals = tx().concepts().getRelationType(relationLabel).roles().map(Type::label).collect(toSet());
        assertTrue(actuals.containsAll(roleLabels));
    }
}

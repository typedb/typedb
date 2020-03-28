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

package hypergraph.test.behaviour.concept.type.entitytype;

import hypergraph.Hypergraph;
import hypergraph.concept.type.EntityType;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import static hypergraph.test.behaviour.connection.ConnectionSteps.sessions;
import static hypergraph.test.behaviour.connection.ConnectionSteps.sessionsToTransactions;
import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;

public class Steps {

    private static Hypergraph.Transaction tx() {
        return sessionsToTransactions.get(sessions.get(0)).get(0);
    }

    @When("put entity type: {word}")
    public void put_entity_type(String type) {
        tx().concepts().putEntityType(type);
    }

    @When("entity\\( ?{word} ?) subtypes: {word}")
    public void entity_subtypes(String type, String superType) {
        EntityType parent = tx().concepts().getEntityType(superType);
        tx().concepts().getEntityType(type).sup(parent);
    }

    @Then("entity\\( ?{word} ?) is null: {bool}")
    public void entity_is_null(String label, boolean isNull) {
        assertEquals(isNull, isNull(tx().concepts().getEntityType(label)));
    }

    @Then("entity\\( ?{word} ?) has super type: {word}")
    public void entity_has_super_type(String type, String superType) {
        EntityType parent = tx().concepts().getEntityType(superType);
        assertEquals(parent, tx().concepts().getEntityType(type).sup());
    }
}

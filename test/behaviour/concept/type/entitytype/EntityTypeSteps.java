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

import io.cucumber.java.en.Then;

import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Behaviour Steps specific to EntityTypes
 */
public class EntityTypeSteps {

    @Then("entity\\( ?{type_label} ?) fails at creating an instance")
    public void entity_type_fails_at_creating_an_instance(String typeLabel) {
        try {
            tx().concepts().getEntityType(typeLabel).create();
            fail();
        } catch (Exception ignore) {
            assertTrue(true);
        }
    }
}

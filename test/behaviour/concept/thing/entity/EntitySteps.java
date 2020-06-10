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

package hypergraph.test.behaviour.concept.thing.entity;

import io.cucumber.java.en.When;

import static hypergraph.test.behaviour.concept.thing.ThingSteps.put;
import static hypergraph.test.behaviour.connection.ConnectionSteps.tx;

public class EntitySteps {

    @When("{var} = entity\\( ?{type_label} ?) create new instance")
    public void entity_type_create_new_instance(String var, String typeLabel) {
        put(var, tx().concepts().getEntityType(typeLabel).create());
    }
}

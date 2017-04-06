/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016  Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package ai.grakn.graql.internal.printer;

import ai.grakn.concept.Concept;
import ai.grakn.graql.internal.hal.HALBuilder;
import mjson.Json;



class HALPrinter extends JsonPrinter {

    @Override
    public Json graqlString(boolean inner, Concept concept) {
        //Todo: remove hardcoded keyspace and think about proper solution (maybe expose the getKeyspace on the concept itself)
        String json = HALBuilder.renderHALConceptData(concept, 1,"grakn",0,100);
        return Json.read(json);
    }

}

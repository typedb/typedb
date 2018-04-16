/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
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

package ai.grakn.engine.controller.util;

/*-
 * #%L
 * grakn-engine
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.engine.controller.response.Attribute;
import ai.grakn.engine.controller.response.AttributeType;
import ai.grakn.engine.controller.response.Concept;
import ai.grakn.engine.controller.response.Entity;
import ai.grakn.engine.controller.response.EntityType;
import ai.grakn.engine.controller.response.MetaConcept;
import ai.grakn.engine.controller.response.Relationship;
import ai.grakn.engine.controller.response.RelationshipType;
import ai.grakn.engine.controller.response.Role;
import ai.grakn.engine.controller.response.Rule;
import ai.grakn.util.Schema;
import com.fasterxml.jackson.databind.ObjectMapper;
import mjson.Json;

import java.io.IOException;

/**
 * <p>
 *     Takes a string representing a {@link ai.grakn.engine.controller.response.Concept} and serialises it into
 *     it's correct object.
 *
 *     We cannot rely on normal jackson deserilisation due to using Jackson and autovalue.
 *     Details described in this issue: https://github.com/FasterXML/jackson-databind/issues/1234
 * </p>
 *
 * @author Filipe Peliz Pintio Teixeira
 */
public class JsonConceptBuilder {
    private static ObjectMapper mapper = new ObjectMapper();

    public static <X extends Concept> X build(Json jsonRepresentation){
        return build(jsonRepresentation.toString());
    }

    public static <X extends Concept> X build(String jsonRepresentation){
        Schema.BaseType baseType = Schema.BaseType.valueOf(Json.read(jsonRepresentation).at("base-type").asString());

        Concept concept;
        try {
            switch (baseType) {
                case ROLE:
                    concept = mapper.readValue(jsonRepresentation, Role.class);
                    break;
                case RULE:
                    concept = mapper.readValue(jsonRepresentation, Rule.class);
                    break;
                case TYPE:
                    concept = mapper.readValue(jsonRepresentation, MetaConcept.class);
                    break;
                case ENTITY_TYPE:
                    concept = mapper.readValue(jsonRepresentation, EntityType.class);
                    break;
                case ENTITY:
                    concept = mapper.readValue(jsonRepresentation, Entity.class);
                    break;
                case ATTRIBUTE_TYPE:
                    concept = mapper.readValue(jsonRepresentation, AttributeType.class);
                    break;
                case ATTRIBUTE:
                    concept = mapper.readValue(jsonRepresentation, Attribute.class);
                    break;
                case RELATIONSHIP_TYPE:
                    concept = mapper.readValue(jsonRepresentation, RelationshipType.class);
                    break;
                case RELATIONSHIP:
                    concept = mapper.readValue(jsonRepresentation, Relationship.class);
                    break;
                default:
                    throw new UnsupportedOperationException(String.format("Cannot wrap object of base type {%s}", baseType));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return (X) concept;
    }
}

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

package ai.grakn.graql.internal.hal;

import ai.grakn.concept.ConceptId;
import ai.grakn.concept.TypeName;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Optional;

import static ai.grakn.graql.internal.hal.HALUtils.BASETYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.EXPLORE_CONCEPT_LINK;
import static ai.grakn.graql.internal.hal.HALUtils.ID_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.INBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.TYPE_PROPERTY;

class HALGeneratedRelation {

    private final RepresentationFactory factory;

    HALGeneratedRelation() {
        this.factory = new StandardRepresentationFactory();
    }

    Representation getNewGeneratedRelation(ConceptId firstID, ConceptId secondID, String assertionID, Optional<TypeName> relationType) {
        Representation representation = factory.newRepresentation(assertionID)
                .withProperty(ID_PROPERTY, "temp-assertion-" + firstID.getValue() + secondID.getValue())
                .withProperty(BASETYPE_PROPERTY, "generated-relation")
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE)
                .withLink(EXPLORE_CONCEPT_LINK, "");

        relationType.ifPresent(typeName -> representation.withProperty(TYPE_PROPERTY, typeName.getValue()));

        return representation;
    }
}

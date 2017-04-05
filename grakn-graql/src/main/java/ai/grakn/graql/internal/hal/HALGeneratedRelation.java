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
import ai.grakn.concept.TypeLabel;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Optional;

class HALGeneratedRelation {

    private final RepresentationFactory factory;

    private final static String ONTOLOGY_LINK = "ontology";
    private final static String INBOUND_EDGE = "IN";

    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";

    HALGeneratedRelation() {
        this.factory = new StandardRepresentationFactory();
    }

    Representation getNewGeneratedRelation(ConceptId firstID, ConceptId secondID, String assertionID, Optional<TypeLabel> relationType) {
        Representation representation = factory.newRepresentation(assertionID)
                .withProperty(ID_PROPERTY, "temp-assertion-" + firstID.getValue() + secondID.getValue())
                .withProperty(BASETYPE_PROPERTY, "generated-relation")
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, "");

        relationType.ifPresent(typeName -> representation.withProperty(TYPE_PROPERTY, typeName.getValue()));

        return representation;
    }
}

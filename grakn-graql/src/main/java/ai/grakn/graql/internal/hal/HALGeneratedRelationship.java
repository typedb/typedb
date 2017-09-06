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

import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import static ai.grakn.graql.internal.hal.HALUtils.BASETYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.EXPLORE_CONCEPT_LINK;
import static ai.grakn.graql.internal.hal.HALUtils.GENERATED_RELATIONSHIP;
import static ai.grakn.graql.internal.hal.HALUtils.ID_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.INFERRED_RELATIONSHIP;
import static ai.grakn.graql.internal.hal.HALUtils.TYPE_PROPERTY;

/**
 * Class used to build the HAL representation of a generated relationship.
 *
 * @author Marco Scoppetta
 */

public class HALGeneratedRelationship {

    private final RepresentationFactory factory;

    public HALGeneratedRelationship() {
        this.factory = new StandardRepresentationFactory();
    }


    Representation getNewGeneratedRelationship(String relationshipId, String relationshipHref, String relationshipType, boolean isInferred) {
        String relationshipBaseType = (isInferred) ? INFERRED_RELATIONSHIP : GENERATED_RELATIONSHIP;
        Representation representation = factory.newRepresentation(relationshipHref)
                .withProperty(ID_PROPERTY, relationshipId)
                .withProperty(BASETYPE_PROPERTY, relationshipBaseType)
                .withLink(EXPLORE_CONCEPT_LINK, "");

        if (!relationshipType.equals("")) {
            representation.withProperty(TYPE_PROPERTY, relationshipType);
        }

        return representation;
    }
}

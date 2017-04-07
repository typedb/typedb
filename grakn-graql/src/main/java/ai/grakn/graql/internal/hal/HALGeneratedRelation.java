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
import static ai.grakn.graql.internal.hal.HALUtils.ID_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.TYPE_PROPERTY;

/**
 * Class used to build the HAL representation of a generated relation.
 *
 * @author Marco Scoppetta
 */

public class HALGeneratedRelation {

    private final RepresentationFactory factory;

    public HALGeneratedRelation() {
        this.factory = new StandardRepresentationFactory();
    }


    Representation getNewGeneratedRelation(String relationId, String relationHref, String relationType, boolean isInferred) {
        String relationBaseType = (isInferred) ? "inferred-relation" : "generated-relation";
        Representation representation = factory.newRepresentation(relationHref)
                .withProperty(ID_PROPERTY, relationId)
                .withProperty(BASETYPE_PROPERTY, relationBaseType)
                .withLink(EXPLORE_CONCEPT_LINK, "");

        if (!relationType.equals("")) {
            representation.withProperty(TYPE_PROPERTY, relationType);
        }

        return representation;
    }
}

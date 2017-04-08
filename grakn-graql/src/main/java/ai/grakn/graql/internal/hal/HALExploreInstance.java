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

import ai.grakn.concept.Concept;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;

import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;

/**
 * Class used to build the HAL representation of a given concept.
 */

class HALExploreInstance extends HALExploreConcept {

    HALExploreInstance(Concept concept, String keyspace, int offset, int limit) {
        super(concept, keyspace, offset, limit);
    }

    void populateEmbedded(Representation halResource, Concept concept) {
        // Instance resources
        concept.asInstance().resources().forEach(currentResource -> {
            Representation embeddedResource = factory.newRepresentation(resourceLinkPrefix + currentResource.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(embeddedResource, currentResource);
            halResource.withRepresentation(currentResource.type().getLabel().getValue(), embeddedResource);
        });
    }


    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

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
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import static ai.grakn.graql.internal.hal.HALUtils.EXPLORE_CONCEPT_LINK;
import static ai.grakn.graql.internal.hal.HALUtils.generateConceptState;
import static ai.grakn.util.REST.WebPath.Concept.CONCEPT;

/**
 * Class used to build the HAL representation of a given concept.
 */

abstract class HALExploreConcept {

    final RepresentationFactory factory;
    final Representation halResource;
    private final String keyspace;
    private final int limit;
    private final int offset;
    final String resourceLinkPrefix;


    HALExploreConcept(Concept concept, String keyspace, int offset, int limit) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = CONCEPT;
        this.keyspace = keyspace;
        this.offset = offset;
        this.limit = limit;
        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId() + getURIParams());

        generateStateAndLinks(halResource, concept);
        populateEmbedded(halResource, concept);

    }

    String getURIParams() {
        // If limit -1, we don't append the limit parameter to the URI string
        String limitParam = (this.limit >= 0) ? "&"+ REST.Request.Concept.LIMIT_EMBEDDED+"=" + this.limit : "";

        return "?"+REST.Request.KEYSPACE+"=" + this.keyspace + "&"+REST.Request.Concept.OFFSET_EMBEDDED+"=" + this.offset + limitParam;
    }

    void generateStateAndLinks(Representation resource, Concept concept) {
        resource.withLink(EXPLORE_CONCEPT_LINK, CONCEPT + concept.getId() + getURIParams());
        generateConceptState(resource, concept);
    }

    abstract void populateEmbedded(Representation halResource, Concept concept);


    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

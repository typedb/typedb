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
import ai.grakn.concept.RelationType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Type;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;

import java.util.stream.Stream;

import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.HAS_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.INBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.PLAYS_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.RELATES_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.SUB_EDGE;


/**
 * Class used to build the HAL representation of a given concept.
 */

class HALExploreType extends HALExploreConcept{

    HALExploreType(Concept concept, String keyspace, int offset, int limit) {
        super(concept, keyspace, offset, limit);
    }

    void populateEmbedded(Representation halResource, Concept concept) {

        Type type = concept.asType();

        // Role types played by current type
        attachRolesPlayed(halResource, type.plays());
        // Resources types owned by the current type
        attachTypeResources(halResource, type);
        // Subtypes
        attachSubTypes(halResource, type);

        if (type.isRelationType()) {
            // Role types that make up this RelationType
            relationTypeRoles(halResource, type.asRelationType());
        } else if (type.isRole()) {
            // Types that can play this role && Relation types this role can take part in.
            roleTypeOntology(halResource, type.asRole());
        }

    }

    private void attachSubTypes(Representation halResource, Type conceptType) {
        conceptType.subs().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getId().equals(conceptType.getId())) {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                generateStateAndLinks(instanceResource, instance);
                halResource.withRepresentation(SUB_EDGE, instanceResource);
            }
        });
    }


    private void roleTypeOntology(Representation halResource, Role role) {
        role.playedByTypes().forEach(type -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + type.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, type);
            halResource.withRepresentation(PLAYS_EDGE, roleRepresentation);
        });

        role.relationTypes().forEach(relType -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + relType.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, relType);
            halResource.withRepresentation(RELATES_EDGE, roleRepresentation);
        });
    }

    private void relationTypeRoles(Representation halResource, RelationType relationType) {
        relationType.relates().forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            //We always return roles with in embedded the entities that play that role.
            roleTypeOntology(roleRepresentation, role);
            halResource.withRepresentation(RELATES_EDGE, roleRepresentation);
        });
    }

    private void attachTypeResources(Representation halResource, Type conceptType) {
        conceptType.attributes().forEach(currentResourceType -> {
            Representation embeddedResource = factory
                    .newRepresentation(resourceLinkPrefix + currentResourceType.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(embeddedResource, currentResourceType);
            halResource.withRepresentation(HAS_EDGE, embeddedResource);
        });
    }

    private void attachRolesPlayed(Representation halResource, Stream<Role> roles) {
        roles.forEach(role -> {
            Representation roleRepresentation = factory
                    .newRepresentation(resourceLinkPrefix + role.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            //We always return roles with in embedded the relations they play in.
            roleTypeOntology(roleRepresentation, role);

            halResource.withRepresentation(PLAYS_EDGE, roleRepresentation);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

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
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Collection;

import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.generateConceptState;

/**
 * Class used to build the HAL representation of a given concept.
 */

class HALConceptOntology {

    private final RepresentationFactory factory;

    private final Representation halResource;

    private final String keyspace;
    private final int limit;
    private final int offset;


    private final String resourceLinkPrefix;
    private final String resourceLinkOntologyPrefix;
    private final static String SUB_EDGE = "sub";
    private final static String ONTOLOGY_LINK = "ontology";
    private final static String OUTBOUND_EDGE = "OUT";
    private final static String INBOUND_EDGE = "IN";
    private final static String RELATES_EDGE = "relates";
    private final static String HAS_RESOURCE_EDGE = "has-resource";
    private final static String PLAYS_ROLE_EDGE = "plays-role";

    // - State properties

    private final static String DIRECTION_PROPERTY = "_direction";


    HALConceptOntology(Concept concept,String keyspace, int offset, int limit) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;
        resourceLinkOntologyPrefix = REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI;
        this.keyspace=keyspace;
        this.offset = offset;
        this.limit = limit;
        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId()+getURIParams());

        handleConceptOntology(halResource, concept);

    }

    private String getURIParams() {
        // If limit -1, we don't append the limit parameter to the URI string
        String limitParam = (this.limit >= 0) ? "&limit=" + this.limit : "";

        return "?keyspace=" + this.keyspace + "&offset="+this.offset+ limitParam;
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId() + getURIParams());
        generateConceptState(resource,concept);
    }

    private void handleConceptOntology(Representation halResource, Concept concept) {
        generateStateAndLinks(halResource, concept);

        if (concept.isType()) {
            attachRolesPlayed(halResource, concept.asType().playsRoles());
            attachTypeResources(halResource, concept.asType());
        }

        if (concept.isRelationType()) {
            relationTypeOntology(halResource, concept.asRelationType());
        } else if (concept.isRoleType()) {
            roleTypeOntology(halResource, concept.asRoleType());
        }

        if (concept.isType()) {
            concept.asType().subTypes().forEach(instance -> {
                // let's not put the current type in its own embedded
                if (!instance.getId().equals(concept.getId())) {
                    Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams())
                            .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                    generateStateAndLinks(instanceResource, instance);
                    halResource.withRepresentation(SUB_EDGE, instanceResource);
                }
            });
        }

        if(concept.isInstance()){
            //attach instance resources
            concept.asInstance().resources().forEach(currentResource -> {
                Representation embeddedResource = factory.newRepresentation(resourceLinkPrefix + currentResource.getId() + getURIParams())
                        .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
                generateStateAndLinks(embeddedResource, currentResource);
                halResource.withRepresentation(currentResource.type().getName().getValue(), embeddedResource);
            });
        }
    }


    private void roleTypeOntology(Representation halResource, RoleType roleType) {
        roleType.playedByTypes().forEach(type -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + type.getId()+getURIParams())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, type);
            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });

        roleType.relationTypes().forEach(relType-> {
                Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + relType.getId() +getURIParams())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                generateStateAndLinks(roleRepresentation, relType);
                halResource.withRepresentation(RELATES_EDGE, roleRepresentation);
            });
    }

    private void relationTypeOntology(Representation halResource, RelationType relationType) {
        relationType.relates().forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId()+getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            //We always return roles with in embedded the entities that play that role.
            roleTypeOntology(roleRepresentation, role);
            halResource.withRepresentation(RELATES_EDGE, roleRepresentation);
        });
    }

    private void attachTypeResources(Representation halResource, Type conceptType){
        conceptType.resources().forEach(currentResourceType -> {
            Representation embeddedResource = factory
                    .newRepresentation(resourceLinkPrefix + currentResourceType.getId() + getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(embeddedResource, currentResourceType);
            halResource.withRepresentation(HAS_RESOURCE_EDGE, embeddedResource);
        });
    }

    private void attachRolesPlayed(Representation halResource, Collection<RoleType> roles) {
        roles.forEach(role -> {
            Representation roleRepresentation = factory
                    .newRepresentation(resourceLinkPrefix + role.getId()+getURIParams())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            //We always return roles with in embedded the relations they play in.
            roleTypeOntology(roleRepresentation, role);

            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

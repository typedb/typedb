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
import ai.grakn.concept.Instance;
import ai.grakn.concept.RelationType;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Type;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Collection;

import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.getBaseType;

/**
 * Class used to build the HAL representation of a given concept.
 */

class HALConceptOntology {

    private RepresentationFactory factory;

    private Representation halResource;

    private final String keyspace;


    private final String resourceLinkPrefix;
    private final String resourceLinkOntologyPrefix;
    private final static String ROOT_CONCEPT = "type";
    private final static String ISA_EDGE = "isa";
    private final static String SUB_EDGE = "sub";
    private final static String ONTOLOGY_LINK = "ontology";
    private final static String OUTBOUND_EDGE = "OUT";
    private final static String INBOUND_EDGE = "IN";
    private final static String HAS_ROLE_EDGE = "has-role";
    private final static String PLAYS_ROLE_EDGE = "plays-role";

    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";
    private final static String VALUE_PROPERTY = "value";


    HALConceptOntology(Concept concept,String keyspace) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;
        resourceLinkOntologyPrefix = REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI;
        this.keyspace="?keyspace="+keyspace;

        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId()+this.keyspace);

        handleConceptOntology(halResource, concept);

    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId());

        //State
        if (concept.isInstance()) {
            Instance instance = concept.asInstance();
            resource.withProperty(ID_PROPERTY, instance.getId())
                    .withProperty(TYPE_PROPERTY, instance.type().getName())
                    .withProperty(BASETYPE_PROPERTY, getBaseType(instance).name());
        } else { // temp fix until a new behaviour is defined
            Type type = concept.asType();
            resource.withProperty(ID_PROPERTY, concept.asType().getName())
                    .withProperty(BASETYPE_PROPERTY, getBaseType(type).name());
        }

        if (concept.isResource()) {
            resource.withProperty(VALUE_PROPERTY, concept.asResource().getValue());
        }
    }

    private void embedType(Representation halResource, Concept concept) {

        // temp fix until a new behaviour is defined
        Representation HALType;
        if (concept.isInstance()) {
            Instance instance = concept.asInstance();
            HALType = factory.newRepresentation(resourceLinkPrefix + instance.type().getId()+this.keyspace)
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(HALType, instance.type());
            halResource.withRepresentation(ISA_EDGE, HALType);
        }

    }

    private void handleConceptOntology(Representation halResource, Concept concept) {
        generateStateAndLinks(halResource, concept);
        embedType(halResource, concept);

        if (concept.isRelationType()) {
            relationTypeOntology(halResource, concept.asRelationType());
        } else if (concept.isRoleType()) {
            roleTypeOntology(halResource, concept.asRoleType());
        } else if (concept.isType()) {
            attachRolesPlayed(halResource, concept.asType().playsRoles());
        }

        if (concept.isType() && concept.asType().superType() != null)
            embedSuperType(halResource, concept.asType());

        if (concept.isType())
            concept.asType().subTypes().forEach(instance -> {
                // let's not put the current type in its own embedded
                if (!instance.getId().equals(concept.getId())) {
                    Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId()+this.keyspace)
                            .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                    generateStateAndLinks(instanceResource, instance);
                    halResource.withRepresentation(SUB_EDGE, instanceResource);
                }
            });
    }

    private void embedSuperType(Representation halResource, Type type) {
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + type.superType().getId()+this.keyspace)
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
        generateStateAndLinks(HALType, type.superType());
        halResource.withRepresentation(SUB_EDGE, HALType);
    }

    private void roleTypeOntology(Representation halResource, RoleType roleType) {
        roleType.playedByTypes().forEach(type -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + type.getId()+this.keyspace)
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, type);
            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });

        RelationType relType = roleType.relationType();
        Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + relType.getId()+this.keyspace)
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        generateStateAndLinks(roleRepresentation, relType);
        halResource.withRepresentation(HAS_ROLE_EDGE, roleRepresentation);

        attachRolesPlayed(halResource, roleType.playsRoles());
    }

    private void relationTypeOntology(Representation halResource, RelationType relationType) {
        relationType.hasRoles().forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId()+this.keyspace)
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            halResource.withRepresentation(HAS_ROLE_EDGE, roleRepresentation);
        });
        attachRolesPlayed(halResource, relationType.playsRoles());
    }

    private void attachRolesPlayed(Representation halResource, Collection<RoleType> roles) {
        roles.forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId()+this.keyspace)
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

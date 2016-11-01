/*
 * MindmapsDB - A Distributed Semantic Database
 * Copyright (C) 2016  Mindmaps Research Ltd
 *
 * MindmapsDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * MindmapsDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with MindmapsDB. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */

package io.mindmaps.engine.visualiser;

import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;
import io.mindmaps.concept.*;
import io.mindmaps.util.REST;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class used to build the HAL representation of a given concept.
 */

public class HALConcept {

    private RepresentationFactory factory;

    private Representation halResource;

    private final String resourceLinkPrefix;
    private final String resourceLinkOntologyPrefix;
    private final Logger LOG = LoggerFactory.getLogger(HALConcept.class);
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

    private boolean embedType;
    private Set<String> typesInQuery = null;

    public HALConcept(Concept concept, int separationDegree, boolean embedTypeParam, Set<String> typesInQuery) {

        embedType = embedTypeParam;
        this.typesInQuery = typesInQuery;
        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;
        resourceLinkOntologyPrefix = REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI;

        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId());

        handleConcept(halResource, concept, separationDegree);

    }


    private void handleConcept(Representation halResource, Concept concept, int separationDegree) {

        generateStateAndLinks(halResource, concept);

        if (embedType && concept.type() != null &&
                (typesInQuery.contains(concept.type().getId())
                        || (concept.type().superType() != null &&
                        typesInQuery.contains(concept.type().superType().getId()))))
            embedType(halResource, concept);

        if (concept.type() == null && typesInQuery.contains(ROOT_CONCEPT))
            embedType(halResource, concept);

        //If a match query contains an assertion we always embed the role players
        if (concept.isRelation() && separationDegree == 0) {
            generateRelationEmbedded(halResource, concept.asRelation(), 1);
        }

        if (separationDegree == 0) return;

        if (concept.isEntity()) {
            generateEntityEmbedded(halResource, concept.asEntity(), separationDegree);
        }
        if (concept.isRelation()) {
            generateRelationEmbedded(halResource, concept.asRelation(), separationDegree);
        }
        if (concept.isResource()) {
            generateOwnerInstances(halResource, concept.asResource(), separationDegree);
        }
        if (concept.isType()) {
            generateTypeEmbedded(halResource, concept.asType(), separationDegree);
        }

    }

    private void generateOwnerInstances(Representation halResource, Resource conceptResource, int separationDegree) {
        final RoleType roleType = conceptResource.type().playsRoles().iterator().next();
        conceptResource.ownerInstances().forEach(object -> {
            Instance currentInstance = (Instance) object;
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + currentInstance.getId())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, currentInstance, separationDegree - 1);
            halResource.withRepresentation(roleType.getId(), instanceResource);
        });
    }

    private void embedType(Representation halResource, Concept concept) {

        // temp fix until a new behaviour is defined
        Representation HALType;
        if (concept.type() != null) {
            HALType = factory.newRepresentation(resourceLinkPrefix + concept.type().getId())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(HALType, concept.type());
            halResource.withRepresentation(ISA_EDGE, HALType);
        } else {
            if (!concept.getId().equals(ROOT_CONCEPT)) {
                HALType = factory.newRepresentation(resourceLinkPrefix + ROOT_CONCEPT);
                HALType.withProperty(ID_PROPERTY, ROOT_CONCEPT)
                        .withProperty(TYPE_PROPERTY, ROOT_CONCEPT)
                        .withProperty(BASETYPE_PROPERTY, ROOT_CONCEPT)
                        .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                        .withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId());
                halResource.withRepresentation(SUB_EDGE, HALType);
            }
        }

    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId());

        //State
        if (concept.isInstance())
            resource.withProperty(ID_PROPERTY, concept.getId())
                    .withProperty(TYPE_PROPERTY, concept.type().getId())
                    .withProperty(BASETYPE_PROPERTY, concept.type().type().getId());
        else // temp fix until a new behaviour is defined
            resource.withProperty(ID_PROPERTY, concept.getId())
                    .withProperty(TYPE_PROPERTY, (concept.type() == null) ? ROOT_CONCEPT : concept.type().getId())
                    .withProperty(BASETYPE_PROPERTY, ROOT_CONCEPT);

        if (concept.isResource()) {
            resource.withProperty("value", concept.asResource().getValue());
        }

        //Resources and links
        if (concept.isEntity()) {
            generateResources(resource, concept.asEntity().resources());
        }

        if (concept.isRelation()) {
            generateResources(resource, concept.asRelation().resources());
        }
    }

    // ================================ resources as HAL state properties ========================= //

    private void generateResources(Representation resource, Collection<Resource<?>> resourcesCollection) {
        final Map<String, JSONArray> resources = new HashMap<>();
        resourcesCollection.forEach(currentResource -> {
            resources.putIfAbsent(currentResource.type().getId(), new JSONArray());
            resources.get(currentResource.type().getId()).put(currentResource.getValue());
        });

        resources.keySet().forEach(current -> resource.withProperty(current, resources.get(current)));
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {

        entity.relations().parallelStream().forEach(rel -> {

            //find the role played by the current instance in the current relation and use the role type as key in the embedded
            String rolePlayedByCurrentConcept = null;
            boolean isResource = false;
            Concept resourceToUse = null;
            for (Map.Entry<RoleType, Instance> entry : rel.rolePlayers().entrySet()) {
                //Some role players can be null
                if (entry.getValue() != null) {
                    if (entry.getValue().isResource()) {
                        isResource = true;
                        resourceToUse = entry.getValue();
                        rolePlayedByCurrentConcept = entry.getKey().getId();
                    } else {
                        if (entry.getValue().getId().equals(entity.getId()))
                            rolePlayedByCurrentConcept = entry.getKey().getId();
                    }
                }
            }

            //If the current relation is to a resource we don't show the assertion, but directly the resource node.
            if (!isResource) {
                attachRelation(halResource, rel, rolePlayedByCurrentConcept, separationDegree);
            }
        });
    }

    private void attachRelation(Representation halResource, Concept rel, String role, int separationDegree) {
        Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId())
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        handleConcept(relationResource, rel, separationDegree - 1);
        halResource.withRepresentation(role, relationResource);
    }


    private void generateRelationEmbedded(Representation halResource, Relation rel, int separationDegree) {
        rel.rolePlayers().forEach((roleType, instance) -> {
            if (instance != null) {
                Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                        .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
                handleConcept(roleResource, instance, separationDegree - 1);
                halResource.withRepresentation(roleType.getId(), roleResource);
            }
        });
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getId().equals(ROOT_CONCEPT)) {
            type.instances().parallelStream().forEach(instance -> {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(ISA_EDGE, instanceResource);
            });
        }
        type.subTypes().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getId().equals(type.getId())) {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(SUB_EDGE, instanceResource);
            }
        });
    }


    // ------ Functions to navigate the ontology ------


    public HALConcept(Concept concept) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;
        resourceLinkOntologyPrefix = REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI;

        factory = new StandardRepresentationFactory();
        typesInQuery = new HashSet<>();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId());
        embedType = true;

        handleConceptOntology(halResource, concept);

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

        if (concept.isType())
            concept.asType().subTypes().forEach(instance -> {
                // let's not put the current type in its own embedded
                if (!instance.getId().equals(concept.getId())) {
                    Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                            .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                    generateStateAndLinks(instanceResource, instance);
                    halResource.withRepresentation(SUB_EDGE, instanceResource);
                }
            });
    }

    private void roleTypeOntology(Representation halResource, RoleType roleType) {
        roleType.playedByTypes().forEach(type -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + type.getId())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, type);
            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });

        RelationType relType = roleType.relationType();
        Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + relType.getId())
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        generateStateAndLinks(roleRepresentation, relType);
        halResource.withRepresentation(HAS_ROLE_EDGE, roleRepresentation);

        attachRolesPlayed(halResource, roleType.playsRoles());
    }

    private void relationTypeOntology(Representation halResource, RelationType relationType) {
        relationType.hasRoles().forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            halResource.withRepresentation(HAS_ROLE_EDGE, roleRepresentation);
        });
        attachRolesPlayed(halResource, relationType.playsRoles());
    }

    private void attachRolesPlayed(Representation halResource, Collection<RoleType> roles) {
        roles.forEach(role -> {
            Representation roleRepresentation = factory.newRepresentation(resourceLinkPrefix + role.getId())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(roleRepresentation, role);
            halResource.withRepresentation(PLAYS_ROLE_EDGE, roleRepresentation);
        });
    }

    //-------------------------------

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }

    public Representation getRepresentation() {
        return halResource;
    }
}

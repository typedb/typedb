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

import ai.grakn.concept.*;
import ai.grakn.util.REST;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class used to build the HAL representation of a given concept.
 */

class HALConceptData {

    private RepresentationFactory factory;

    private Representation halResource;

    private final String resourceLinkPrefix;
    private final String resourceLinkOntologyPrefix;
    private final static String ISA_EDGE = "isa";
    private final static String SUB_EDGE = "sub";
    private final static String ONTOLOGY_LINK = "ontology";
    private final static String OUTBOUND_EDGE = "OUT";
    private final static String INBOUND_EDGE = "IN";
    private final static String ROOT_CONCEPT = "type";


    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";
    private final static String VALUE_PROPERTY = "value";

    private boolean embedType;
    private Set<String> typesInQuery = null;

    HALConceptData(Concept concept, int separationDegree, boolean embedTypeParam, Set<String> typesInQuery) {

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
                (typesInQuery.contains(concept.type().getName())
                        || (concept.type().superType() != null &&
                        typesInQuery.contains(concept.type().superType().getName()))))
            embedType(halResource, concept);

        if (concept.type() == null)
            embedType(halResource, concept);

        if (concept.isType() && concept.asType().superType() != null)
            embedSuperType(halResource, concept.asType());

        //If a match query contains an assertion we always embed the role players
        if (concept.isRelation() && separationDegree == 0) {
            generateRelationEmbedded(halResource, concept.asRelation(), 1);
        }

        if (concept.isRule()) {
            generateRuleLHS(halResource, concept.asRule());
            generateRuleRHS(halResource, concept.asRule());
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

    private void generateRuleRHS(Representation halResource, Rule rule) {
        Representation RHS = factory.newRepresentation(resourceLinkPrefix + "RHS-"+rule.getId())
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix)
                .withProperty(ID_PROPERTY, "RHS-"+rule.getId())
                .withProperty(TYPE_PROPERTY, "RHS")
                .withProperty(BASETYPE_PROPERTY, "resource-type")
                .withProperty(VALUE_PROPERTY,rule.getRHS().admin().toString());
        halResource.withRepresentation("RHS", RHS);
    }

    private void generateRuleLHS(Representation halResource, Rule rule) {
        Representation LHS = factory.newRepresentation(resourceLinkPrefix + "LHS-"+rule.getId())
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix)
                .withProperty(ID_PROPERTY, "LHS-"+rule.getId())
                .withProperty(TYPE_PROPERTY, "LHS")
                .withProperty(BASETYPE_PROPERTY, "resource-type")
                .withProperty(VALUE_PROPERTY,rule.getLHS().admin().asConjunction()
                        .getPatterns()
                        .stream().map(Object::toString).collect(Collectors.joining("; \n")));
        halResource.withRepresentation("LHS", LHS);
    }

    private void generateOwnerInstances(Representation halResource, Resource conceptResource, int separationDegree) {
        final RoleType roleType = conceptResource.type().playsRoles().iterator().next();
        conceptResource.ownerInstances().forEach(object -> {
            Instance currentInstance = (Instance) object;
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + currentInstance.getId())
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, currentInstance, separationDegree - 1);
            halResource.withRepresentation(roleType.getName(), instanceResource);
        });
    }

    private void embedSuperType(Representation halResource, Type type) {
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + type.superType().getId())
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
        generateStateAndLinks(HALType, type.superType());
        halResource.withRepresentation(SUB_EDGE, HALType);
    }

    private void embedType(Representation halResource, Concept concept) {

        // temp fix until a new behaviour is defined
        Representation HALType;

        if (concept.type() != null) {
            HALType = factory.newRepresentation(resourceLinkPrefix + concept.type().getId())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(HALType, concept.type());
            halResource.withRepresentation(ISA_EDGE, HALType);
        }
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId());

        //State
        if (concept.isInstance())
            resource.withProperty(ID_PROPERTY, concept.getId())
                    .withProperty(TYPE_PROPERTY, concept.type().getName())
                    .withProperty(BASETYPE_PROPERTY, concept.type().type().getName());
        else // temp fix until a new behaviour is defined
            resource.withProperty(ID_PROPERTY, concept.asType().getName())
                    .withProperty(TYPE_PROPERTY, (concept.type() == null) ? ROOT_CONCEPT : concept.type().getName())
                    .withProperty(BASETYPE_PROPERTY, ROOT_CONCEPT);

        if (concept.isResource()) {
            resource.withProperty(VALUE_PROPERTY, concept.asResource().getValue());
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
        resourcesCollection.forEach(currentResource -> {
            Representation embeddedResource = factory.newRepresentation(resourceLinkPrefix + currentResource.getId())
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(embeddedResource, currentResource);
            resource.withRepresentation(currentResource.type().getName(), embeddedResource);
        });
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {

        entity.relations().parallelStream().forEach(rel -> {

            //find the role played by the current instance in the current relation and use the role type as key in the embedded
            String rolePlayedByCurrentConcept = null;
            boolean isResource = false;
            for (Map.Entry<RoleType, Instance> entry : rel.rolePlayers().entrySet()) {
                //Some role players can be null
                if (entry.getValue() != null) {
                    if (entry.getValue().isResource()) {
                        isResource = true;
                    } else {
                        if (entry.getValue().getId().equals(entity.getId()))
                            rolePlayedByCurrentConcept = entry.getKey().getName();
                    }
                }
            }
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
                halResource.withRepresentation(roleType.getName(), roleResource);
            }
        });
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getName().equals(ROOT_CONCEPT)) {
            type.instances().parallelStream().forEach(instance -> {

                if (instance.isType() && instance.asType().isImplicit()) return;

                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(ISA_EDGE, instanceResource);
            });
        }
        type.subTypes().stream().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getName().equals(type.getName())) {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId())
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(SUB_EDGE, instanceResource);
            }
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }

    Representation getRepresentation() {
        return halResource;
    }
}

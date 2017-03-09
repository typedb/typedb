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
import ai.grakn.concept.Entity;
import ai.grakn.concept.Instance;
import ai.grakn.concept.Relation;
import ai.grakn.concept.Resource;
import ai.grakn.concept.RoleType;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Type;
import ai.grakn.concept.TypeName;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.hal.HALConceptRepresentationBuilder.generateConceptState;

/**
 * Class used to build the HAL representation of a given concept.
 */

class HALConceptData {

    private final RepresentationFactory factory;

    private final Representation halResource;

    private final String resourceLinkPrefix;
    private final String resourceLinkOntologyPrefix;
    private final String keyspace;
    private final static String ISA_EDGE = "isa";
    private final static String SUB_EDGE = "sub";
    private final static String ONTOLOGY_LINK = "ontology";
    private final static String OUTBOUND_EDGE = "OUT";
    private final static String INBOUND_EDGE = "IN";


    // - State properties

    private final static String ID_PROPERTY = "_id";
    private final static String TYPE_PROPERTY = "_type";
    private final static String BASETYPE_PROPERTY = "_baseType";
    private final static String DIRECTION_PROPERTY = "_direction";
    private final static String VALUE_PROPERTY = "_value";

    private final boolean embedType;
    private final Set<TypeName> typesInQuery;

    private final int offset;
    private final int limit;

    HALConceptData(Concept concept, int separationDegree, boolean embedTypeParam, Set<TypeName> typesInQuery, String keyspace, int offset, int limit) {

        embedType = embedTypeParam;
        this.typesInQuery = typesInQuery;
        this.offset = offset;
        this.limit = limit;
        this.keyspace = keyspace;
        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;
        resourceLinkOntologyPrefix = REST.WebPath.CONCEPT_BY_ID_ONTOLOGY_URI;

        factory = new StandardRepresentationFactory();

        //If we will include embedded nodes and limit is >=0 we increase the offset to prepare URI for next request
        int uriOffset = (separationDegree > 0 && limit >= 0) ? (offset + limit) : offset;

        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId() + getURIParams(uriOffset));

        handleConcept(halResource, concept, separationDegree);

    }

    private String getURIParams(int offset) {
        // If limit -1, we don't append the limit parameter to the URI string
        String limitParam = (this.limit >= 0) ? "&limit=" + this.limit : "";

        return "?keyspace=" + this.keyspace + "&offset=" + offset + limitParam;
    }


    private void handleConcept(Representation halResource, Concept concept, int separationDegree) {

        generateStateAndLinks(halResource, concept);

        if (embedType && concept.isInstance()) {
            Instance instance = concept.asInstance();
            if (typesInQuery.contains(instance.type().getName())
                    || (instance.type().superType() != null &&
                    typesInQuery.contains(instance.type().superType().getName()))) {
                embedType(halResource, instance);
            }
        }

        if (concept.isType() && concept.asType().superType() != null) {
            embedSuperType(halResource, concept.asType());
        }

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
            //Only when double clicking on a specific relation we want to fetch also the other relations the current one plays a role into.
            embedRelationsPlaysRole(halResource, concept.asRelation());
        }
        if (concept.isResource()) {
            generateOwnerInstances(halResource, concept.asResource(), separationDegree);
        }

        if (concept.isType()) {
            generateTypeEmbedded(halResource, concept.asType(), separationDegree);
        }

    }

    private void generateRuleRHS(Representation halResource, Rule rule) {
        Representation RHS = factory.newRepresentation(resourceLinkPrefix + "RHS-" + rule.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix)
                .withProperty(ID_PROPERTY, "RHS-" + rule.getId().getValue())
                .withProperty(TYPE_PROPERTY, "RHS")
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.RESOURCE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getRHS().admin().toString());
        halResource.withRepresentation("RHS", RHS);
    }

    private void generateRuleLHS(Representation halResource, Rule rule) {
        Representation LHS = factory.newRepresentation(resourceLinkPrefix + "LHS-" + rule.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix)
                .withProperty(ID_PROPERTY, "LHS-" + rule.getId().getValue())
                .withProperty(TYPE_PROPERTY, "LHS")
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.RESOURCE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getLHS().admin().toString());
        halResource.withRepresentation("LHS", LHS);
    }

    private void generateOwnerInstances(Representation halResource, Resource<?> conceptResource, int separationDegree) {
        final TypeName roleType = conceptResource.type().getName();
        conceptResource.ownerInstances().forEach(instance -> {
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, instance, separationDegree - 1);
            halResource.withRepresentation(roleType.getValue(), instanceResource);
        });
    }

    private void embedSuperType(Representation halResource, Type type) {
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + type.superType().getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
        generateStateAndLinks(HALType, type.superType());
        halResource.withRepresentation(SUB_EDGE, HALType);
    }

    private void embedType(Representation halResource, Instance instance) {

        // temp fix until a new behaviour is defined
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + instance.type().getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);

        generateStateAndLinks(HALType, instance.type());
        halResource.withRepresentation(ISA_EDGE, HALType);
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(ONTOLOGY_LINK, resourceLinkOntologyPrefix + concept.getId() + getURIParams(0));

        generateConceptState(resource, concept);

        //Resources and links
        if (concept.isInstance()) {
            generateResources(resource, concept.asInstance().resources());
        }
    }

    // ================================ resources as HAL state properties ========================= //

    private void generateResources(Representation resource, Collection<Resource<?>> resourcesCollection) {
        resourcesCollection.forEach(currentResource -> {
            Representation embeddedResource = factory.newRepresentation(resourceLinkPrefix + currentResource.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
            generateStateAndLinks(embeddedResource, currentResource);
            resource.withRepresentation(currentResource.type().getName().getValue(), embeddedResource);
        });
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {

        Stream<Relation> relationStream = entity.relations().stream().skip(offset);
        if (limit >= 0) relationStream = relationStream.limit(limit);
        relationStream.forEach(rel -> {

            //find the role played by the current instance in the current relation and use the role type as key in the embedded
            for (Map.Entry<RoleType, Set<Instance>> entry : rel.allRolePlayers().entrySet()) {
                //Some role players can be null
                boolean relationAttached = false;
                for (Instance instance : entry.getValue()) {
                    if (instance != null && !instance.isResource() && instance.getId().equals(entity.getId())) {
                        attachRelation(halResource, rel, entry.getKey().getName(), separationDegree);
                        relationAttached = true;
                        break;
                    }
                }
                if(relationAttached) break;
            }
        });
    }

    private void attachRelation(Representation halResource, Concept rel, TypeName role, int separationDegree) {
        Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        handleConcept(relationResource, rel, separationDegree - 1);
        halResource.withRepresentation(role.getValue(), relationResource);
    }


    private void generateRelationEmbedded(Representation halResource, Relation rel, int separationDegree) {

        rel.allRolePlayers().forEach((roleType, instanceSet) -> {
            instanceSet.forEach(instance -> {
                if (instance != null) {
                    Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                            .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
                    handleConcept(roleResource, instance, separationDegree - 1);
                    halResource.withRepresentation(roleType.getName().getValue(), roleResource);
                }
            });
        });
    }

    private void embedRelationsPlaysRole(Representation halResource, Relation rel) {
        rel.playsRoles().forEach(roleTypeRel -> {
            rel.relations(roleTypeRel).forEach(relation -> {
                Representation relationRepresentation = factory.newRepresentation(resourceLinkPrefix + relation.getId() + getURIParams(0))
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(relationRepresentation, relation, 0);
                halResource.withRepresentation(roleTypeRel.getName().getValue(), relationRepresentation);
            });
        });
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getName().equals(Schema.MetaSchema.CONCEPT.getName())) {
            Stream<? extends Instance> instancesStream = type.instances().stream().filter(instance -> (!instance.isType() || !instance.asType().isImplicit())).skip(offset);
            if (limit >= 0) instancesStream = instancesStream.limit(limit);
            instancesStream.forEach(instance -> {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(ISA_EDGE, instanceResource);
            });
        }
        // We only limit the number of instances and not subtypes.
        type.subTypes().stream().filter(instance -> (!instance.getName().equals(type.getName()))).forEach(instance -> {
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, instance, separationDegree - 1);
            halResource.withRepresentation(SUB_EDGE, instanceResource);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }

    Representation getRepresentation() {
        return halResource;
    }
}

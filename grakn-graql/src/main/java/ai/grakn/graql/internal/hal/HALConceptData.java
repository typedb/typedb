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
import ai.grakn.concept.TypeLabel;
import ai.grakn.util.REST;
import ai.grakn.util.Schema;
import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static ai.grakn.graql.internal.hal.HALUtils.BASETYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.DIRECTION_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.EXPLORE_CONCEPT_LINK;
import static ai.grakn.graql.internal.hal.HALUtils.ID_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.INBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.ISA_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.OUTBOUND_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.SUB_EDGE;
import static ai.grakn.graql.internal.hal.HALUtils.TYPE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.VALUE_PROPERTY;
import static ai.grakn.graql.internal.hal.HALUtils.generateConceptState;
import static ai.grakn.util.REST.WebPath.Dashboard.EXPLORE;


/**
 * Class used to build the HAL representation of a given concept.
 * @author Marco Scoppetta
 */

public class HALConceptData {

    private final RepresentationFactory factory;

    private final Representation halResource;

    private final String resourceLinkPrefix;
    private final String keyspace;

    private final boolean embedType;
    private final Set<TypeLabel> typesInQuery;

    private final int offset;
    private final int limit;


    public HALConceptData(Concept concept, int separationDegree, boolean embedTypeParam, Set<TypeLabel> typesInQuery, String keyspace, int offset, int limit){

        embedType = embedTypeParam;
        this.typesInQuery = typesInQuery;
        this.offset = offset;
        this.limit = limit;
        this.keyspace = keyspace;
        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.Concept.CONCEPT;

        factory = new StandardRepresentationFactory();

        //If we will include embedded nodes and limit is >=0 we increase the offset to prepare URI for next request
        int uriOffset = (separationDegree > 0 && limit >= 0) ? (offset + limit) : offset;

        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId() + getURIParams(uriOffset));

        handleConcept(halResource, concept, separationDegree);

    }

    private String getURIParams(int offset) {
        // If limit -1, we don't append the limit parameter to the URI string
        String limitParam = (this.limit >= 0) ? "&"+REST.Request.Concept.LIMIT_EMBEDDED+"=" + this.limit : "";

        return "?"+REST.Request.KEYSPACE+"=" + this.keyspace + "&"+REST.Request.Concept.OFFSET_EMBEDDED+"=" + offset + limitParam;
    }


    private void handleConcept(Representation halResource, Concept concept, int separationDegree) {

        generateStateAndLinks(halResource, concept);

        if (embedType && concept.isInstance()) {
            Instance instance = concept.asInstance();
            if (typesInQuery.contains(instance.type().getLabel())
                    || (instance.type().superType() != null &&
                    typesInQuery.contains(instance.type().superType().getLabel()))) {
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
            embedRelationsPlays(halResource, concept.asRelation());
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
                .withLink(EXPLORE_CONCEPT_LINK, EXPLORE)
                .withProperty(ID_PROPERTY, "RHS-" + rule.getId().getValue())
                .withProperty(TYPE_PROPERTY, "RHS")
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.RESOURCE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getRHS().admin().toString());
        halResource.withRepresentation("RHS", RHS);
    }

    private void generateRuleLHS(Representation halResource, Rule rule) {
        Representation LHS = factory.newRepresentation(resourceLinkPrefix + "LHS-" + rule.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(EXPLORE_CONCEPT_LINK, EXPLORE)
                .withProperty(ID_PROPERTY, "LHS-" + rule.getId().getValue())
                .withProperty(TYPE_PROPERTY, "LHS")
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.RESOURCE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getLHS().admin().toString());
        halResource.withRepresentation("LHS", LHS);
    }

    private void generateOwnerInstances(Representation halResource, Resource<?> conceptResource, int separationDegree) {
        final TypeLabel roleType = conceptResource.type().getLabel();
        Stream<Instance> ownersStream = conceptResource.ownerInstances().stream().skip(offset);
        if (limit >= 0) ownersStream = ownersStream.limit(limit);
        ownersStream.forEach(instance -> {
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

        Representation HALType = factory.newRepresentation(resourceLinkPrefix + instance.type().getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);

        generateStateAndLinks(HALType, instance.type());
        halResource.withRepresentation(ISA_EDGE, HALType);
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(EXPLORE_CONCEPT_LINK, EXPLORE + concept.getId() + getURIParams(0));
        generateConceptState(resource, concept);
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {
        Stream<Relation> relationStream = entity.relations().stream();

        relationStream = relationStream.skip(offset);
        if (limit >= 0) relationStream = relationStream.limit(limit);


        relationStream.forEach(rel -> {
            embedRelationsNotConnectedToResources(halResource, entity, rel, separationDegree);
        });
    }

    private void attachRelation(Representation halResource, Concept rel, TypeLabel role, int separationDegree) {
        Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        handleConcept(relationResource, rel, separationDegree - 1);
        halResource.withRepresentation(role.getValue(), relationResource);
    }


    private void generateRelationEmbedded(Representation halResource, Relation rel, int separationDegree) {

        rel.allRolePlayers().forEach((roleType, instanceSet) -> {
            instanceSet.forEach(instance -> {
                // Relations attached to relations are handled in embedRelationsPlaysRole method.
                // We filter out relations to resources.
                if (instance != null && !instance.isRelation()) {
                    Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                            .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
                    handleConcept(roleResource, instance, separationDegree - 1);
                    halResource.withRepresentation(roleType.getLabel().getValue(), roleResource);
                }
            });
        });
    }

    private void embedRelationsNotConnectedToResources(Representation halResource, Concept concept, Relation relation, int separationDegree) {
        TypeLabel rolePlayedByCurrentConcept = null;
        boolean isResource = false;
        for (Map.Entry<RoleType, Set<Instance>> entry : relation.allRolePlayers().entrySet()) {
            for (Instance instance : entry.getValue()) {
                //Some role players can be null
                if (instance != null) {
                    if (instance.isResource()) {
                        isResource = true;
                    } else if (instance.getId().equals(concept.getId())) {
                        rolePlayedByCurrentConcept = entry.getKey().getLabel();
                    }
                }
            }
        }
        if (!isResource) {
            attachRelation(halResource, relation, rolePlayedByCurrentConcept, separationDegree);
        }
    }

    private void embedRelationsPlays(Representation halResource, Relation rel) {
        rel.plays().forEach(roleTypeRel -> {
            rel.relations(roleTypeRel).forEach(relation -> {
                embedRelationsNotConnectedToResources(halResource, rel, relation, 1);
            });
        });
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getLabel().equals(Schema.MetaSchema.CONCEPT.getLabel())) {
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
        type.subTypes().stream().filter(instance -> (!instance.getLabel().equals(type.getLabel()))).forEach(instance -> {
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, instance, separationDegree - 1);
            halResource.withRepresentation(SUB_EDGE, instanceResource);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }

    public Representation getRepresentation() {
        return halResource;
    }
}

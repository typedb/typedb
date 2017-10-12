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

import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.Concept;
import ai.grakn.concept.Entity;
import ai.grakn.concept.Label;
import ai.grakn.concept.Relationship;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.Thing;
import ai.grakn.concept.Type;
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
    private final Keyspace keyspace;

    private final boolean embedType;
    private final Set<Label> typesInQuery;

    private final int offset;
    private final int limit;


    public HALConceptData(Concept concept, int separationDegree, boolean embedTypeParam, Set<Label> typesInQuery, Keyspace keyspace, int offset, int limit){

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

        if (embedType && concept.isThing()) {
            Thing thing = concept.asThing();
            if (typesInQuery.contains(thing.type().getLabel())
                    || (thing.type().sup() != null &&
                    typesInQuery.contains(thing.type().sup().getLabel()))) {
                embedType(halResource, thing);
            }
        }

        if (concept.isType() && concept.asType().sup() != null) {
            embedSuperType(halResource, concept.asType());
        }

        //If a get query contains an assertion we always embed the role players
        if (concept.isRelationship() && separationDegree == 0) {
            generateRelationEmbedded(halResource, concept.asRelationship(), 1);
        }

        if (concept.isRule() && !Schema.MetaSchema.isMetaLabel(concept.asRule().getLabel())) {
            generateRuleLHS(halResource, concept.asRule());
            generateRuleRHS(halResource, concept.asRule());
        }

        if (separationDegree == 0) return;

        if (concept.isEntity()) {
            generateEntityEmbedded(halResource, concept.asEntity(), separationDegree);
        }

        if (concept.isRelationship()) {
            generateRelationEmbedded(halResource, concept.asRelationship(), separationDegree);
            //Only when double clicking on a specific relation we want to fetch also the other relations the current one plays a role into.
            embedRelationsPlays(halResource, concept.asRelationship());
        }
        if (concept.isAttribute()) {
            generateOwnerInstances(halResource, concept.asAttribute(), separationDegree);
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
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.ATTRIBUTE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getThen().admin().toString());
        halResource.withRepresentation("RHS", RHS);
    }

    private void generateRuleLHS(Representation halResource, Rule rule) {
        Representation LHS = factory.newRepresentation(resourceLinkPrefix + "LHS-" + rule.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE)
                .withLink(EXPLORE_CONCEPT_LINK, EXPLORE)
                .withProperty(ID_PROPERTY, "LHS-" + rule.getId().getValue())
                .withProperty(TYPE_PROPERTY, "LHS")
                .withProperty(BASETYPE_PROPERTY, Schema.BaseType.ATTRIBUTE_TYPE.name())
                .withProperty(VALUE_PROPERTY, rule.getWhen().admin().toString());
        halResource.withRepresentation("LHS", LHS);
    }

    private void generateOwnerInstances(Representation halResource, Attribute<?> conceptAttribute, int separationDegree) {
        final Label roleType = conceptAttribute.type().getLabel();
        Stream<Thing> ownersStream = conceptAttribute.ownerInstances().skip(offset);
        if (limit >= 0) ownersStream = ownersStream.limit(limit);
        ownersStream.forEach(instance -> {
            Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(instanceResource, instance, separationDegree - 1);
            halResource.withRepresentation(roleType.getValue(), instanceResource);
        });
    }

    private void embedSuperType(Representation halResource, Type type) {
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + type.sup().getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
        generateStateAndLinks(HALType, type.sup());
        halResource.withRepresentation(SUB_EDGE, HALType);
    }

    private void embedType(Representation halResource, Thing thing) {

        Representation HALType = factory.newRepresentation(resourceLinkPrefix + thing.type().getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);

        generateStateAndLinks(HALType, thing.type());
        halResource.withRepresentation(ISA_EDGE, HALType);
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        resource.withLink(EXPLORE_CONCEPT_LINK, EXPLORE + concept.getId() + getURIParams(0));
        generateConceptState(resource, concept);
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {
        Stream<Relationship> relationStream = entity.relationships();

        relationStream = relationStream.skip(offset);
        if (limit >= 0) relationStream = relationStream.limit(limit);

        relationStream.forEach(rel -> embedRelationsNotConnectedToAttributes(halResource, entity, rel, separationDegree));
    }

    private void attachRelation(Representation halResource, Concept rel, Label role, int separationDegree) {
        Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId() + getURIParams(0))
                .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
        handleConcept(relationResource, rel, separationDegree - 1);
        halResource.withRepresentation(role.getValue(), relationResource);
    }


    private void generateRelationEmbedded(Representation halResource, Relationship rel, int separationDegree) {
        rel.allRolePlayers().forEach((roleType, instanceSet) -> {
            instanceSet.forEach(instance -> {
                if (instance != null) {
                    Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                            .withProperty(DIRECTION_PROPERTY, OUTBOUND_EDGE);
                    if (!instance.isRelationship()) {
                        handleConcept(roleResource, instance, separationDegree - 1);
                    } else {
                        // If instance is a relation we just add state properties to HAL representation
                        // without including its role players.
                        generateStateAndLinks(roleResource, instance);
                    }
                    halResource.withRepresentation(roleType.getLabel().getValue(), roleResource);
                }
            });
        });
    }

    private void embedRelationsNotConnectedToAttributes(Representation halResource, Concept concept, Relationship relationship, int separationDegree) {
        Label rolePlayedByCurrentConcept = null;
        boolean isAttribute = false;
        for (Map.Entry<Role, Set<Thing>> entry : relationship.allRolePlayers().entrySet()) {
            for (Thing thing : entry.getValue()) {
                //Some role players can be null
                if (thing != null) {
                    if (thing.isAttribute()) {
                        isAttribute = true;
                    } else if (thing.getId().equals(concept.getId())) {
                        rolePlayedByCurrentConcept = entry.getKey().getLabel();
                    }
                }
            }
        }
        if (!isAttribute) {
            attachRelation(halResource, relationship, rolePlayedByCurrentConcept, separationDegree);
        }
    }

    private void embedRelationsPlays(Representation halResource, Relationship rel) {
        rel.plays().forEach(roleTypeRel -> {
            rel.relationships(roleTypeRel).forEach(relation -> {
                embedRelationsNotConnectedToAttributes(halResource, rel, relation, 1);
            });
        });
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getLabel().equals(Schema.MetaSchema.THING.getLabel())) {
            Stream<? extends Thing> instancesStream = type.instances().skip(offset);
            if (limit >= 0) instancesStream = instancesStream.limit(limit);
            instancesStream.forEach(instance -> {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId() + getURIParams(0))
                        .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(ISA_EDGE, instanceResource);
            });
        }
        // We only limit the number of instances and not subtypes.
        type.subs().filter(sub -> (!sub.getLabel().equals(type.getLabel()))).forEach(sub -> {
            Representation subResource = factory.newRepresentation(resourceLinkPrefix + sub.getId() + getURIParams(0))
                    .withProperty(DIRECTION_PROPERTY, INBOUND_EDGE);
            handleConcept(subResource, sub, separationDegree - 1);
            halResource.withRepresentation(SUB_EDGE, subResource);
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }

    public Representation getRepresentation() {
        return halResource;
    }
}

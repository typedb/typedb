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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Class used to build the HAL representation of a given concept.
 */

public class HALConcept {

    private RepresentationFactory factory;

    private Representation halResource;

    private final String resourceLinkPrefix;
    private final Logger LOG = LoggerFactory.getLogger(HALConcept.class);
    private final String ROOT_CONCEPT = "concept-type";
    private final String ISA_EDGE = "isa";


    public HALConcept(Concept concept, int separationDegree) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core
        resourceLinkPrefix = REST.WebPath.CONCEPT_BY_ID_URI;

        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId());

        try {
            handleConcept(halResource, concept, separationDegree);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleConcept(Representation halResource, Concept concept, int separationDegree) {

        generateStateAndLinks(halResource, concept);

        embedType(halResource, concept);

        if (separationDegree == 0) return;


        if (concept.isEntity()) {
            generateEntityEmbedded(halResource, concept.asEntity(), separationDegree);
        }
        if (concept.isRelation()) {
            generateRelationEmbedded(halResource, concept.asRelation(), separationDegree);
        }

        if (concept.isType()) {
            generateTypeEmbedded(halResource, concept.asType(), separationDegree);
        }

    }

    private void embedType(Representation halResource, Concept concept) {
        Representation HALType = factory.newRepresentation(resourceLinkPrefix + concept.type().getId());
        generateStateAndLinks(HALType, concept.type());
        halResource.withRepresentation(ISA_EDGE, HALType);
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        //State
        resource.withProperty("_id", concept.getId())
                .withProperty("_type", concept.type().getId())
                .withProperty("_baseType", concept.type().type().getId());

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
        final Map<String, Collection<String>> resources = new HashMap<>();
        resourcesCollection.forEach(currentResource -> {
            resources.putIfAbsent(currentResource.type().getId(), new HashSet<>());
            resources.get(currentResource.type().getId()).add(currentResource.getValue().toString());
        });

        resources.keySet().forEach(current -> resource.withProperty(current, resources.get(current).toString()));
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
                attachAssertion(halResource,rel,rolePlayedByCurrentConcept,separationDegree);
            } else {
                attachRelation(halResource,resourceToUse,rolePlayedByCurrentConcept);
            }
        });
    }

    private void attachAssertion(Representation halResource, Concept rel, String role, int separationDegree){
        Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId());
        handleConcept(relationResource, rel, separationDegree - 1);
        halResource.withRepresentation(role, relationResource);
    }

    private void attachRelation(Representation halResource, Concept resourceToUse, String role){
        Representation resourceResource = factory.newRepresentation(resourceLinkPrefix + resourceToUse.getId());
        handleConcept(resourceResource, resourceToUse.asResource(), 0);
        halResource.withRepresentation(role, resourceResource);
    }


    private void generateRelationEmbedded(Representation halResource, Relation rel, int separationDegree) {

        rel.rolePlayers().forEach((roleType, instance) -> {
            Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
            handleConcept(roleResource, instance, separationDegree - 1);
            halResource.withRepresentation(roleType.getId(), roleResource);
        });

    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getId().equals(ROOT_CONCEPT)) {
            type.instances().forEach(instance -> {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(instance.getId(), instanceResource);
            });
        }
        type.subTypes().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getId().equals(type.getId())) {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                handleConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(instance.getId(), instanceResource);
            }
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

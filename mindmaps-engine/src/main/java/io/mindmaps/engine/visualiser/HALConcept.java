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
import io.mindmaps.engine.util.ConfigProperties;
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


    public HALConcept(Concept concept) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core

        ConfigProperties prop = ConfigProperties.getInstance();
        int separationDegree = prop.getPropertyAsInt(ConfigProperties.HAL_DEGREE_PROPERTY);
        resourceLinkPrefix = "http://" + prop.getProperty(ConfigProperties.SERVER_HOST_NAME) + ":"
                + prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER)
                + REST.WebPath.CONCEPT_BY_ID_URI;

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

        if (separationDegree == 0) return;

        embedType(halResource, concept);

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
        handleConcept(HALType, concept.type(), 0);
        halResource.withRepresentation(ISA_EDGE, HALType);
    }

    private void generateStateAndLinks(Representation resource, Concept concept) {

        //State
        resource.withProperty("_id", concept.getId())
                .withProperty("_type", concept.type().getId())
                .withProperty("_baseType", concept.type().type().getId());


        //Resources and links
        if (concept.isEntity()) {
            generateResources(resource, concept.asEntity().resources());
            generateEntityLinks(resource, concept.asEntity());
        }

        if (concept.isRelation()) {
            generateResources(resource, concept.asRelation().resources());
            generateRelationLinks(resource, concept.asRelation());
        }

        if (concept.isType()) {
            generateTypeLinks(resource, concept.asType());
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


    // ======================================= _links =============================================== //

    private void generateTypeLinks(Representation halResource, Type type) {
        if (!type.getId().equals(ROOT_CONCEPT)) {
            type.instances().forEach(instance -> halResource.withLink(instance.getId(), resourceLinkPrefix + instance.getId()));
        }
        type.subTypes().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getId().equals(type.getId())) {
                halResource.withLink(instance.getId(), resourceLinkPrefix + instance.getId());
            }
        });
    }

    private void generateRelationLinks(Representation halResource, Relation rel) {
        rel.rolePlayers().forEach((roleType, instance) -> {
            halResource.withLink(roleType.getId(), resourceLinkPrefix + instance.getId());
        });
    }

    private void generateEntityLinks(Representation halResource, Entity entity) {
        for (Relation rel : entity.relations()) {
            String rolePlayedByCurrentConcept = null;
            for (Map.Entry<RoleType, Instance> entry : rel.rolePlayers().entrySet()) {
                if (entry.getValue().getId().equals(entity.getId()))
                    rolePlayedByCurrentConcept = entry.getKey().getId();
            }
            halResource.withLink(rolePlayedByCurrentConcept, resourceLinkPrefix + rel.getId());
        }
    }

    // ======================================= _embedded ================================================//


    private void generateEntityEmbedded(Representation halResource, Entity entity, int separationDegree) {

        for (Relation rel : entity.relations()) {

            Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId());

            //find the role played by the current instance in the current relation and use the role type as key in the embedded
            String rolePlayedByCurrentConcept = null;
            for (Map.Entry<RoleType, Instance> entry : rel.rolePlayers().entrySet()) {
                if (entry.getValue().getId().equals(entity.getId()))
                    rolePlayedByCurrentConcept = entry.getKey().getId();
            }

            handleConcept(relationResource, rel, separationDegree - 1);
            halResource.withRepresentation(rolePlayedByCurrentConcept, relationResource);
        }
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

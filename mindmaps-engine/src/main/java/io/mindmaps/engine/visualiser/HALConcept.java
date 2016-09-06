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
import io.mindmaps.constants.RESTUtil;
import io.mindmaps.core.model.*;
import io.mindmaps.engine.util.ConfigProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;


public class HALConcept {

    RepresentationFactory factory;

    Representation halResource;

    private final String resourceLinkPrefix;
    private final Logger LOG = LoggerFactory.getLogger(HALConcept.class);


    //TODO: check if separationDegree can go below 0

    public HALConcept(Concept concept) {

        //building HAL concepts using: https://github.com/HalBuilder/halbuilder-core

        ConfigProperties prop = ConfigProperties.getInstance();
        int separationDegree = prop.getPropertyAsInt(ConfigProperties.HAL_DEGREE_PROPERTY);
        resourceLinkPrefix = "http://" + prop.getProperty(ConfigProperties.SERVER_HOST_NAME) + ":"
                + prop.getProperty(ConfigProperties.SERVER_PORT_NUMBER)
                + RESTUtil.WebPath.CONCEPT_BY_ID_URI;

        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId());

        try {
            dispatchConcept(halResource, concept, separationDegree);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void generateState(Representation resource, Concept concept) {
        resource.withProperty("_id", concept.getId())
                .withProperty("_type", concept.type().getId())
                .withProperty("_baseType", concept.type().type().getId());

        if (concept.isEntity()) {
            entityResources(halResource, concept.asEntity());
        }

        if (concept.isRelation()) {
            relationResources(halResource, concept.asRelation());
        }
    }

    private void dispatchConcept(Representation halResource, Concept concept, int separationDegree) {

        generateState(halResource, concept);

        if (separationDegree == 0) return;

        // If it's an instance we put resources as state variables
        if (concept.isEntity()) {
            generateEntityLinks(halResource, concept.asEntity());
            generateEntityEmbedded(halResource, concept.asEntity(), separationDegree);
        }
        if (concept.isRelation()) {
            generateRelationLinks(halResource, concept.asRelation());
            generateRelationEmbedded(halResource, concept.asRelation(), separationDegree);
        }

        if (concept.isType()) {
            generateTypeLinks(halResource, concept.asType());
            generateTypeEmbedded(halResource, concept.asType(), separationDegree);
        }

    }

    // ================================ resources as HAL state properties ========================= //

    private void entityResources(Representation resource, Entity entity) {
        final Map<String, Collection<String>> resources = new HashMap<>();
        entity.resources().forEach(currentResource -> {
            resources.putIfAbsent(currentResource.type().getId(), new HashSet<>());
            resources.get(currentResource.type().getId()).add(currentResource.getValue().toString());
        });

        resources.keySet().forEach(current -> resource.withProperty(current, resources.get(current).toString()));
    }

    private void relationResources(Representation resource, Relation rel) {
        rel.resources().forEach(currentResource -> resource.withProperty(currentResource.getId(), currentResource.getValue()));
    }

    // ======================================= _links =============================================== //

    private void generateTypeLinks(Representation halResource, Type type) {
        if (!type.getId().equals("type")) {
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

        if (separationDegree == 0) return;

        for (Relation rel : entity.relations()) {
            //generate embedded for role
            Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId());
            generateState(relationResource, rel);
            generateRelationLinks(relationResource, rel);

            //find the role played by the current instance in the current relation and use the role type as key in the embedded
            String rolePlayedByCurrentConcept = null;
            for (Map.Entry<RoleType, Instance> entry : rel.rolePlayers().entrySet()) {
                if (entry.getValue().getId().equals(entity.getId()))
                    rolePlayedByCurrentConcept = entry.getKey().getId();
            }
            generateRelationEmbedded(relationResource, rel, separationDegree - 1);
            halResource.withRepresentation(rolePlayedByCurrentConcept, relationResource);
        }
    }


    private void generateRelationEmbedded(Representation halResource, Relation rel, int separationDegree) {
        if (separationDegree == 0) return;
        try {
            rel.rolePlayers().forEach((roleType, instance) -> {
                Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                dispatchConcept(roleResource, instance, separationDegree - 1);
                halResource.withRepresentation(roleType.getId(), roleResource);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void generateTypeEmbedded(Representation halResource, Type type, int separationDegree) {
        if (!type.getId().equals("type")) {
            type.instances().forEach(instance -> {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                dispatchConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(instance.getId(), instanceResource);
            });
        }
        type.subTypes().forEach(instance -> {
            // let's not put the current type in its own embedded
            if (!instance.getId().equals(type.getId())) {
                Representation instanceResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                dispatchConcept(instanceResource, instance, separationDegree - 1);
                halResource.withRepresentation(instance.getId(), instanceResource);
            }
        });
    }

    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

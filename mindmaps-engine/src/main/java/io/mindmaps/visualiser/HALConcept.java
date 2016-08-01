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

package io.mindmaps.visualiser;

import com.theoryinpractise.halbuilder.api.Representation;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.standard.StandardRepresentationFactory;
import io.mindmaps.util.ConfigProperties;
import io.mindmaps.core.model.*;


public class HALConcept {

    RepresentationFactory factory;

    Representation halResource;

    String resourceLinkPrefix;


    public HALConcept(Concept concept) {


        int separationDegrees = Integer.parseInt(ConfigProperties.getInstance().getProperty(ConfigProperties.HAL_DEGREE_PROPERTY));
        resourceLinkPrefix=ConfigProperties.HAL_RESOURCE_PREFIX;

        factory = new StandardRepresentationFactory();
        halResource = factory.newRepresentation(resourceLinkPrefix + concept.getId());

        generateState(halResource, concept);
        if (concept.isEntity()) {
            entityResources(halResource, concept.asEntity()); // put resources as state variables
            generateInstanceEmbedded(halResource, concept, separationDegrees);
        }
        if (concept.isRelation()) {
            relationResources(halResource, concept.asRelation()); // put resources as state variables
            generateRelationEmbedded(halResource, concept.asRelation());
        }
        if (concept.isType()) {
            generateTypeEmbedded(halResource, concept.asType());
        }


    }

    private void entityResources(Representation resource, Entity entity) {
        entity.resources().forEach(resource1 -> {
            resource.withProperty(resource1.type().getId(), resource1.getValue());
        });
    }

    private void relationResources(Representation resource, Relation rel) {
        rel.resources().forEach(resource1 -> {
            resource.withProperty(resource1.getId(), resource1.getValue());
        });
    }

    private void generateState(Representation resource, Concept concept) {
        resource.withProperty("_id", concept.getId())
                .withProperty("_value", (concept.getValue() != null) ? concept.getValue().toString() : "")
                .withProperty("_type", concept.type().getId())
                .withProperty("_baseType", concept.type().type().getId());
    }

    private void generateLinks() {
    }


    // populate _embedded field for Instance
    private void generateInstanceEmbedded(Representation halResource, Concept concept, int separationDegree) {

        if (separationDegree == 0) return;

        for (Relation rel : concept.asInstance().relations()) {
            final String[] rolePlayedByCurrentConcept = {""};

            Representation relationResource = factory.newRepresentation(resourceLinkPrefix + rel.getId());
            generateState(relationResource, rel);

            if (!isRelationToResource(rel)) {
                rel.rolePlayers().forEach((roleType, instance) -> {
                    Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
                    HALConcept.this.generateState(roleResource, instance);
                    if (instance.getId().equals(concept.getId())) rolePlayedByCurrentConcept[0] = roleType.getId();
                    relationResource.withRepresentation(instance.getId(), roleResource);
                });

                generateInstanceEmbedded(halResource, rel, separationDegree - 1);
                halResource.withRepresentation(rolePlayedByCurrentConcept[0], relationResource);
            }
        }
    }

    private boolean isRelationToResource(Relation rel) {
        boolean isResource = false;
        for (RoleType role : rel.rolePlayers().keySet()) {
            Instance instance = rel.rolePlayers().get(role);
            if (instance.isResource()) isResource = true;
        }
        return isResource;
    }


    // populate _embedded field for a Relation
    private void generateRelationEmbedded(Representation halResource, Relation rel) {

        rel.rolePlayers().forEach((roleType, instance) -> {
            Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
            generateState(roleResource, instance);
            halResource.withRepresentation(roleType.getId(), roleResource);
        });

    }

    // populate _embedded field for Type
    private void generateTypeEmbedded(Representation halResource, Type type) {

        type.instances().forEach(instance -> {
            Representation roleResource = factory.newRepresentation(resourceLinkPrefix + instance.getId());
            generateState(roleResource, instance);
            halResource.withRepresentation(instance.getId(), roleResource);
        });

    }


    public String render() {
        return halResource.toString(RepresentationFactory.HAL_JSON);
    }
}

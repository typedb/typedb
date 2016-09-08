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

package io.mindmaps.graql.internal.validation;

import io.mindmaps.util.ErrorMessage;
import io.mindmaps.MindmapsGraph;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * A validator for validating resource types
 */
class ResourceValidator implements Validator {

    private final Collection<String> resourceTypes;

    /**
     * @param resourceTypes a list of resource type IDs to validate
     */
    ResourceValidator(Collection<String> resourceTypes) {
        this.resourceTypes = resourceTypes;
    }

    @Override
    public Stream<String> getErrors(MindmapsGraph graph) {
        return resourceTypes.stream().flatMap(r -> validateResource(graph, r));
    }

    /**
     * @param graph the graph to look up the resource type in
     * @param resourceType the resource type to validate
     * @return a stream of errors regarding this resource type
     */
    private Stream<String> validateResource(MindmapsGraph graph, String resourceType) {
        if (graph.getResourceType(resourceType) == null) {
            return Stream.of(ErrorMessage.MUST_BE_RESOURCE_TYPE.getMessage(resourceType));
        } else {
            return Stream.empty();
        }
    }
}

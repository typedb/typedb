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

package io.mindmaps.graql.internal.parser;

import io.mindmaps.core.model.Concept;
import io.mindmaps.core.model.Resource;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;

/**
 * A getter is used to return a string representation of a property of a Concept, using the resultString method
 */
@FunctionalInterface
public interface Getter {

    /**
     * @param result the concept to extract a property from
     * @return the string representation of the property
     */
    String resultString(Concept result);

    /**
     * @return a getter that will get the id of a concept
     */
    static Getter id() {
        return result -> " " + colorKeyword("id") + " \"" + result.getId() + "\"";
    }

    /**
     * @return a getter that will get the value of a concept
     */
    static Getter value() {
        return result -> Optional.ofNullable(result.getValue()).map(
                property -> " " + colorKeyword("value") + " \"" + property + "\""
        ).orElse("");
    }

    /**
     * @return a getter that will get the id of the type of a concept
     */
    static Getter isa() {
        return result -> colorKeyword(" isa ") + colorType(result.type().getId());
    }

    /**
     * @param resourceType a resource type id
     * @return a getter that will get all resources' values of a given type of a concept
     */
    static Getter has(String resourceType) {
        String hasType = colorKeyword("has ") + colorType(resourceType);

        return result -> {
            StringBuilder str = new StringBuilder();
            resources(result).stream()
                    .filter(r -> r.type().getId().equals(resourceType))
                    .forEach(r -> str.append(" ").append(hasType).append(" \"").append(r.getValue()).append("\""));
            return str.toString();
        };
    }

    /**
     * @return a getter that will get the left-hand side property of a concept
     */
    static Getter lhs() {
        return result -> colorKeyword(" lhs") + " \"" + result.asRule().getLHS() + "\"";
    }

    /**
     * @return a getter that will get the right-hand side property of a concept
     */
    static Getter rhs() {
        return result -> colorKeyword(" rhs") + " \"" + result.asRule().getRHS() + "\"";
    }

    /**
     * This method is necessary because the 'resource' method appears in 3 separate interfaces
     * @param concept a concept to get the resources of
     * @return a collection of resources, or an empty collection if the concept can't have resources
     */
    static Collection<Resource<?>> resources(Concept concept) {
        if (concept.isEntity()) {
            return concept.asEntity().resources();
        } else if (concept.isRelation()) {
            return concept.asRelation().resources();
        } else if (concept.isRule()) {
            return concept.asRule().resources();
        } else {
            return new HashSet<>();
        }
    }

    /**
     * @param keyword a keyword to color-code using ANSI colors
     * @return the keyword, color-coded
     */
    static String colorKeyword(String keyword) {
        return ANSI.color(keyword, ANSI.BLUE);
    }

    /**
     * @param type a type to color-code using ANSI colors
     * @return the type, color-coded
     */
    static String colorType(String type) {
        return ANSI.color(type, ANSI.PURPLE);
    }
}

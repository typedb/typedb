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

package ai.grakn.engine.controller;

import ai.grakn.concept.Concept;
import ai.grakn.concept.RoleType;
import ai.grakn.engine.GraknEngineConfig;
import mjson.Json;
import spark.Request;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static ai.grakn.engine.GraknEngineConfig.DEFAULT_KEYSPACE_PROPERTY;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static java.util.stream.Collectors.toList;

/**
 * Methods that will be used by all of the controllers.
 *
 * @author Alexandra Orth
 */
public class Utilities {
    private final static GraknEngineConfig properties = GraknEngineConfig.getInstance();
    private static final String defaultKeyspace = properties.getProperty(DEFAULT_KEYSPACE_PROPERTY);
    private static final String RELATION_TYPES = "/graph/match?query=match $a isa %s id '%s'; ($a,$b) isa %s; limit %s;&keyspace=%s&limit=%s&reasoner=true";
    private static final String ENTITY_TYPES = "/graph/match?query=match $a isa %s id '%s'; $b isa %s; ($a,$b); limit %s;&keyspace=%s&limit=%s&reasoner=true";
    private static final String ROLE_TYPES = "/graph/match?query=match $a isa %s id '%s'; ($a,%s:$b); limit %s;&keyspace=%s&limit=%s&reasoner=true";

    public static String getKeyspace(Request request) {
        String keyspace = request.queryParams(KEYSPACE_PARAM);
        return keyspace == null ? defaultKeyspace : keyspace;
    }

    static List<Json> getRelationTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .map(relationType -> relationType.getName().getValue()).sorted()
                .map(relationName -> Json.object("value", relationName, "href", String.format(RELATION_TYPES, concept.asInstance().type().getName().getValue(), concept.getId().getValue(), relationName, limit, keyspace, limit)))
                .collect(toList());
    }

    static List<Json> getEntityTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .flatMap(relationType -> relationType.hasRoles().stream().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .flatMap(roleType -> roleType.playedByTypes().stream().map(entityType -> entityType.getName().getValue()))
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(entityName -> Json.object("value", entityName, "href", String.format(ENTITY_TYPES, concept.asInstance().type().getName().getValue(), concept.getId().getValue(), entityName, limit, keyspace, limit)))
                .collect(toList());
    }

    static List<Json> getRoleTypes(Collection<RoleType> roleTypesPlayerByConcept, Concept concept, int limit, String keyspace) {
        return roleTypesPlayerByConcept.stream().flatMap(roleType -> roleType.relationTypes().stream())
                .flatMap(relationType -> relationType.hasRoles().stream().filter(roleType1 -> !roleTypesPlayerByConcept.contains(roleType1)))
                .map(roleType -> roleType.getName().getValue())
                .collect(Collectors.toSet()).stream()
                .sorted()
                .map(roleName -> Json.object("value", roleName, "href", String.format(ROLE_TYPES, concept.asInstance().type().getName().getValue(), concept.getId().getValue(), roleName, limit, keyspace, limit)))
                .collect(toList());
    }

    public static String getAcceptType(Request request) {
        return request.headers("Accept").split(",")[0];
    }

    public static String getAsString(String property, String request) {
        Json json = Json.read(request);
        return json.has(property) ? json.at(property).asString() : null;
    }

    public static List<String> getAsList(String property, String request) {
        Json json = Json.read(request);
        return json.has(property)
                ? json.at(property).asList().stream().map(Object::toString).collect(toList())
                : null;
    }
}

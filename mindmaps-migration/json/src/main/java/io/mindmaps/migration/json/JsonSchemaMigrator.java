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

package io.mindmaps.migration.json;


import io.mindmaps.MindmapsGraph;
import io.mindmaps.concept.EntityType;
import io.mindmaps.concept.ResourceType;
import io.mindmaps.concept.RoleType;
import io.mindmaps.concept.Type;
import mjson.Json;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static io.mindmaps.concept.ResourceType.DataType.STRING;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Migrator for migrating a JSON schema into a Mindmaps ontology
 */
public class JsonSchemaMigrator {

    private final MindmapsGraph graph;

    private static final String ID_SUFFIX = ".json#";

    // WARNING: This regex cannot perfectly match all emails because the definition is context-free
    private static final String EMAIL_REGEX =
            "([!#-'*+/-9=?A-Z^-~-]+(\\.[!#-'*+/-9=?A-Z^-~-]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")@" +
                    "([!#-'*+/-9=?A-Z^-~-]+(\\.[!#-'*+/-9=?A-Z^-~-]+)*|\\[[\t -Z^-~]*])";

    /**
     * Create a JsonSchemaMigrator to migrate into the given graph
     * @param graph a graph to migrate an ontology into
     */
    public JsonSchemaMigrator(MindmapsGraph graph) {
        this.graph = graph;
    }

    /**
     * Migrate a JSON schema into a mindmaps ontology
     * @param schema a JSON schema
     * @return the root concept type representing the schema
     */
    public Type migrateSchema(Json.Schema schema) {
        String id = schema.getJson().at("id").asString();
        if (id.startsWith("/")) id = id.substring(1);
        String name = id.substring(0, id.length() - ID_SUFFIX.length()).replace("/", "-");
        return migrateSchema(name, schema);
    }

    /**
     * Migrate a JSON schema into a mindmaps ontology
     * @param name the name of the schema
     * @param schema a JSON schema
     * @return the root concept type representing the schema
     */
    public Type migrateSchema(String name, Json.Schema schema) {
        return migrateSchema(name, schema.getJson());
    }

    private Type migrateSchema(String name, Json schema) {
        Set<JsonType> types = getTypes(schema).collect(toSet());

        // Check if any subtypes are resource types
        boolean isResource = types.stream().anyMatch(JsonType::isResourceType);

        if (types.size() == 0) {
            return graph.putEntityType(name);
        } else if (types.size() == 1) {
            return migrateByType(name, schema, types.iterator().next(), isResource);
        } else {
            Type conceptType;
            if (isResource) {
                conceptType = graph.putResourceType(name, JsonType.STRING.getDatatype());
            } else {
                conceptType = graph.putEntityType(name);
            }

            // Create a subtype for each permitted type
            List<Type> conceptTypes = types.stream()
                    .map(type -> migrateByType(JsonMapper.subtypeName(conceptType, type), schema, type, isResource))
                    .collect(toList());

            for (Type subType : conceptTypes) {

                if(subType.isResourceType()){
                    subType.asResourceType().superType(conceptType.asResourceType());
                } else if(subType.isEntityType()){
                    subType.asEntityType().superType(conceptType.asEntityType());
                }
            }

            return conceptType;
        }
    }

    /**
     * Migrate part of a JSON schema and produce a concept or resource type representing it
     */
    private Type migrateByType(String name, Json schema, JsonType type, boolean isResource) {
        Type conceptType = isResource ? migrateResourceType(name, type) : graph.putEntityType(name);

        switch (type) {
            case OBJECT:
                migrateJsonObject(conceptType, schema);
                break;
            case ARRAY:
                migrateJsonArray(conceptType, schema);
                break;
            case NULL:
                break;
            case STRING:
                migrateJsonString(conceptType, schema);
        }

        return conceptType;
    }

    private ResourceType migrateResourceType(String name, JsonType type){
        if(type.getDatatype() == null){
           return graph.putResourceType(name, STRING);
        }
        return graph.putResourceType(name, type.getDatatype());
    }

    private void migrateJsonString(Type conceptType, Json schema) {
        if (schema.has("enum")) {
            // Transform an enum into an alternation regex
            String regex = schema.at("enum").asJsonList().stream().map(Json::asString).collect(joining("|"));
            conceptType.asResourceType().setRegex(regex);
        }

        if (schema.has("minLength") && schema.has("maxLength")) {
            int minLength = schema.at("minLength").asInteger();
            int maxLength = schema.at("maxLength").asInteger();
            String regex = String.format(".{%s,%s}", minLength, maxLength);
            conceptType.asResourceType().setRegex(regex);
        }

        if (schema.is("format", "email")) {
            conceptType.asResourceType().setRegex(EMAIL_REGEX);
        }
    }

    /**
     * Migrate a JSON object by adding a type and relation for every property
     */
    private void migrateJsonObject(Type type, Json schema) {
        // Add all properties contained in this JSON object
        Map<String, Json> properties = schema.at("properties", Json.object()).asJsonMap();
        properties.forEach((key, property) -> migrateProperty(type, key, property));
    }

    /**
     * Migrate a JSON array, by adding a concept type to represent items inside it
     */
    private void migrateJsonArray(Type type, Json schema) {
        // Add one property for items within array
        migrateProperty(type, JsonMapper.arrayItemName(type), schema.at("items"));
    }

    /**
     * Migrate an inner property of a JSON object or array, into a concept type, plus a relation type connecting them
     */
    private void migrateProperty(Type owner, String propertyName, Json propertySchema) {
        // Create the property concept type
        Type propertyType = migrateSchema(propertyName, propertySchema);

        if(propertyType.isResourceType()){
            RoleType roleOwner = graph.putRoleType(JsonMapper.roleOwnerResourceName(propertyType));
            RoleType roleOther = graph.putRoleType(JsonMapper.roleOtherResourceName(propertyType));
            graph.putRelationType(JsonMapper.resourceRelationName(propertyType)).hasRole(roleOwner).hasRole(roleOther);

            owner.playsRole(roleOwner);
            propertyType.playsRole(roleOther);
        }
        else {
            // Add a relation connecting the owner to its property
            RoleType roleOwner = graph.putRoleType(JsonMapper.roleOwnerName(propertyType));
            RoleType roleOther = graph.putRoleType(JsonMapper.roleOtherName(propertyType));
            graph.putRelationType(JsonMapper.relationName(propertyType)).hasRole(roleOwner).hasRole(roleOther);

            owner.playsRole(roleOwner);
            propertyType.playsRole(roleOther);
        }
    }

    /**
     * Get the set of types from a Json 'type' property. Can be either empty, a single type or a list of types
     */
    private Stream<JsonType> getTypes(Json json) {
        Stream<Json> types;

        Json type = json.at("type");
        Json oneOf = json.at("oneOf");

        if (oneOf != null) {
            return oneOf.asJsonList().stream().flatMap(this::getTypes);
        } else if (type == null) {
            types = Stream.empty();
        } else if (type.isString()) {
            types = Stream.of(type);
        } else {
            types = type.asJsonList().stream();
        }

        return types.map(Json::asString).map(JsonType::getType);
    }
}

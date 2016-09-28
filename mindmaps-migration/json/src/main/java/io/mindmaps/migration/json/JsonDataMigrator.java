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
import io.mindmaps.concept.*;
import io.mindmaps.concept.ResourceType.DataType;
import mjson.Json;

import java.util.UUID;

/**
 * Migrator for migrating JSON data into Mindmaps instances
 */
public class JsonDataMigrator {

    private MindmapsGraph graph;

    /**
     * Create a JsonDataMigrator to migrate into the given graph
     */
    public JsonDataMigrator() {}

    public JsonDataMigrator graph(MindmapsGraph graph){
        this.graph = graph;
        return this;
    }


    /**
     * Migrate JSON data into Mindmaps instances
     * @param name the name of the root concept type
     * @param data the data to load into Mindmaps
     * @return the instance representing the data
     */
    public Instance migrateData(String name, Json data) {
        Type type = graph.getType(name);

        return migrateData(type, data);
    }

    private Instance migrateData(Type type, Json data) {
        JsonType jsonType = JsonType.getType(data);

        // If the type has subtypes, then we choose the correct one based on the JSON data type
        if (type.subTypes().size() > 1) {
            type = graph.getType(JsonMapper.subtypeName(type, jsonType));
        }

        boolean isResource = type.isResourceType();//|| type.subTypes().stream().anyMatch(Type::isResourceType);
        Instance instance = isResource ?
                migrateResource(jsonType, type.asResourceType(), data) : migrateEntity(type.asEntityType());

        switch (jsonType) {
            case OBJECT:
                migrateJsonObject(instance, data);
                break;
            case ARRAY:
                migrateJsonArray(instance, data);
                break;
            case NULL:
                break;
        }

        return instance;
    }

    private void migrateJsonObject(Instance instance, Json data) {
        data.asJsonMap().forEach((key, value) -> migrateProperty(instance, key, value));
    }

    private void migrateJsonArray(Instance instance, Json data) {
        String propertyName = JsonMapper.arrayItemName(instance.type());
        data.asJsonList().forEach(item -> migrateProperty(instance, propertyName, item));
    }

    private void migrateProperty(Instance instance, String propertyName, Json data) {
        Type propertyType = graph.getType(propertyName);

        RelationType relationType;
        RoleType roleOwner;
        RoleType roleOther;
        if(propertyType.isResourceType()) {
            relationType = graph.getRelationType(JsonMapper.resourceRelationName(propertyType));
            roleOwner = graph.getRoleType(JsonMapper.roleOwnerResourceName(propertyType));
            roleOther = graph.getRoleType(JsonMapper.roleOtherResourceName(propertyType));
        }
        else {
            relationType = graph.getRelationType(JsonMapper.relationName(propertyType));
            roleOwner = graph.getRoleType(JsonMapper.roleOwnerName(propertyType));
            roleOther = graph.getRoleType(JsonMapper.roleOtherName(propertyType));
        }

        Instance propertyInstance = migrateData(propertyType, data);

        Relation relation = graph.putRelation(UUID.randomUUID().toString(), relationType);
        relation.putRolePlayer(roleOwner, instance);
        relation.putRolePlayer(roleOther, propertyInstance);
    }

    private Instance migrateEntity(EntityType type){
        String id = UUID.randomUUID().toString();
        return graph.putEntity(id, type);
    }

    private Instance migrateResource(JsonType jsonType, ResourceType type, Json data){

        switch (jsonType) {
            case STRING:
                return migrateJsonString(type, data);
            case NUMBER:
            case INTEGER:
                return migrateJsonNumber(type, data);
            case BOOLEAN:
                return migrateJsonBoolean(type, data);
        }

        return graph.putResource("", type);
    }

    private Resource migrateJsonString(ResourceType type, Json data) {
        return graph.putResource(data.asString(), type);
    }

    private Resource migrateJsonNumber(ResourceType type, Json data) {
        DataType datatype = type.getDataType();
        Object value;
        if (JsonType.INTEGER.getDatatype().equals(datatype)) {
            value = data.asLong();
        } else {
            value = data.asDouble();
        }
        return graph.putResource(value, type);
    }

    private Resource migrateJsonBoolean(ResourceType type,  Json data) {
        return graph.putResource(Boolean.toString(data.asBoolean()), type);
    }
}

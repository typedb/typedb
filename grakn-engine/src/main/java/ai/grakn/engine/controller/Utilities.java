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

import ai.grakn.engine.util.ConfigProperties;
import mjson.Json;
import spark.Request;

import java.util.List;

import static ai.grakn.engine.util.ConfigProperties.DEFAULT_KEYSPACE_PROPERTY;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static java.util.stream.Collectors.toList;

/**
 * Methods that will be used by all of the controllers.
 */
public abstract class Utilities {

    private final static ConfigProperties properties = ConfigProperties.getInstance();
    private static final String defaultKeyspace = properties.getProperty(DEFAULT_KEYSPACE_PROPERTY);

    public static String getKeyspace(Request request){
        String keyspace = request.queryParams(KEYSPACE_PARAM);
        return keyspace == null ? defaultKeyspace : keyspace;
    }

    public static String getAcceptType(Request request){
        return request.headers("Accept").split(",")[0];
    }

    public static String getAsString(String property, String request){
        Json json = Json.read(request);
       return json.has(property) ? json.at(property).asString() : null;
    }

    public static List<String> getAsList(String property, String request){
        Json json = Json.read(request);
        return json.has(property)
                ? json.at(property).asList().stream().map(Object::toString).collect(toList())
                : null;
    }
}

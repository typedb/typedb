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


import ai.grakn.GraknGraph;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Entity;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Resource;
import ai.grakn.concept.ResourceType;
import ai.grakn.engine.GraknEngineConfig;
import ai.grakn.exception.GraknEngineServerException;
import ai.grakn.factory.EngineGraknGraphFactory;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.util.ErrorMessage;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.Collection;
import java.util.Properties;

import static ai.grakn.engine.GraknEngineConfig.FACTORY_ANALYTICS;
import static ai.grakn.engine.GraknEngineConfig.FACTORY_INTERNAL;
import static ai.grakn.util.REST.GraphConfig.COMPUTER;
import static ai.grakn.util.REST.GraphConfig.DEFAULT;
import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.WebPath.System.CONFIGURATION;
import static ai.grakn.util.REST.WebPath.System.KEYSPACES;


/**
 * <p>
 *     Controller Providing Configs for building Grakn Graphs
 * </p>
 *
 * <p>
 *     When calling {@link ai.grakn.Grakn#session(String, String)} and using the non memory location this controller
 *     is accessed. The controller provides the necessary config needed in order to build a {@link ai.grakn.GraknGraph}.
 *
 *     This controller also allows the retrieval of all keyspaces opened so far.
 * </p>
 *
 * @author fppt
 */
public class SystemController {
    private final Logger LOG = LoggerFactory.getLogger(SystemController.class);

    public SystemController(Service spark) {
        spark.get(KEYSPACES,     this::getKeyspaces);
        spark.get(CONFIGURATION, this::getConfiguration);
    }

    @GET
    @Path("/configuration")
    @ApiOperation(value = "Get config which is used to build graphs")
    @ApiImplicitParam(name = "graphConfig", value = "The type of graph config to return", required = true, dataType = "string", paramType = "path")
    private String getConfiguration(Request request, Response response) {
        String graphConfig = request.queryParams(GRAPH_CONFIG_PARAM);

        // Make a copy of the properties object
        Properties properties = new Properties();
        properties.putAll(GraknEngineConfig.getInstance().getProperties());

        // Get the correct factory based on the request
        switch ((graphConfig != null) ? graphConfig : DEFAULT) {
            case DEFAULT:
                break; // Factory is already correctly set
            case COMPUTER:
                properties.setProperty(FACTORY_INTERNAL, properties.get(FACTORY_ANALYTICS).toString());
                break;
            default:
                throw new GraknEngineServerException(500, "Unrecognised graph config: " + graphConfig);
        }

        // Turn the properties into a Json object
        Json config = Json.make(properties);

        // Remove the JWT Secret
        config.delAt(GraknEngineConfig.JWT_SECRET_PROPERTY);

        return config.toString();
    }

    @GET
    @Path("/keyspaces")
    @ApiOperation(value = "Get all the key spaces that have been opened")
    private String getKeyspaces(Request request, Response response) {
        try (GraknGraph graph = EngineGraknGraphFactory.getInstance().getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            ResourceType<String> keyspaceName = graph.getType(SystemKeyspace.KEYSPACE_RESOURCE);
            Json result = Json.array();
            if (graph.getType(SystemKeyspace.KEYSPACE_ENTITY) == null) {
                LOG.warn("No system ontology in system keyspace, possibly a bug!");
                return result.toString();
            }
            for (Entity keyspace : graph.<EntityType>getType(SystemKeyspace.KEYSPACE_ENTITY).instances()) {
                Collection<Resource<?>> names = keyspace.resources(keyspaceName);
                if (names.size() != 1) {
                    throw new GraknEngineServerException(500,
                            ErrorMessage.INVALID_SYSTEM_KEYSPACE.getMessage(" keyspace " + keyspace.getId() + " has no unique name."));
                }
                result.add(names.iterator().next().getValue());
            }
            return result.toString();
        } catch (Exception e) {
            LOG.error("While retrieving keyspace list:", e);
            throw new GraknEngineServerException(500, e);
        }
    }
}

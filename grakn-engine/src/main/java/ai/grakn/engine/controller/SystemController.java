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
import static ai.grakn.engine.GraknEngineConfig.FACTORY_ANALYTICS;
import static ai.grakn.engine.GraknEngineConfig.FACTORY_INTERNAL;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.exception.GraknServerException;
import ai.grakn.factory.SystemKeyspace;
import ai.grakn.util.ErrorMessage;
import static ai.grakn.util.REST.GraphConfig.COMPUTER;
import static ai.grakn.util.REST.GraphConfig.DEFAULT;
import static ai.grakn.util.REST.Request.GRAPH_CONFIG_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.WebPath.System.CONFIGURATION;
import static ai.grakn.util.REST.WebPath.System.KEYSPACES;
import static ai.grakn.util.REST.WebPath.System.METRICS;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;


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
    private final EngineGraknGraphFactory factory;
    private final MetricRegistry metricRegistry;
    private final ObjectMapper mapper;
    private final DropwizardExports prometheusMetricWrapper;
    private final CollectorRegistry prometheusRegistry;

    public SystemController(EngineGraknGraphFactory factory, Service spark,
            MetricRegistry metricRegistry) {
        this.factory = factory;
        this.metricRegistry = metricRegistry;
        this.prometheusMetricWrapper = new DropwizardExports(metricRegistry);
        this.prometheusRegistry = new CollectorRegistry();
        prometheusRegistry.register(prometheusMetricWrapper);
        spark.get(KEYSPACES,     this::getKeyspaces);
        spark.get(CONFIGURATION, this::getConfiguration);
        spark.get(METRICS, this::getMetrics);

        final TimeUnit rateUnit = TimeUnit.SECONDS;
        final TimeUnit durationUnit = TimeUnit.SECONDS;
        final boolean showSamples = false;
        MetricFilter filter = MetricFilter.ALL;

        this.mapper = new ObjectMapper().registerModule(
                new MetricsModule(rateUnit,
                    durationUnit,
                    showSamples,
                    filter));
    }

    @GET
    @Path("/configuration")
    @ApiOperation(value = "Get config which is used to build graphs")
    @ApiImplicitParam(name = "graphConfig", value = "The type of graph config to return", required = true, dataType = "string", paramType = "path")
    private String getConfiguration(Request request, Response response) {
        String graphConfig = request.queryParams(GRAPH_CONFIG_PARAM);

        // Make a copy of the properties object
        Properties properties = new Properties();
        properties.putAll(factory.properties());

        // Get the correct factory based on the request
        switch ((graphConfig != null) ? graphConfig : DEFAULT) {
            case DEFAULT:
                break; // Factory is already correctly set
            case COMPUTER:
                properties.setProperty(FACTORY_INTERNAL, properties.get(FACTORY_ANALYTICS).toString());
                break;
            default:
                throw GraknServerException.internalError("Unrecognised graph config: " + graphConfig);
        }

        // Turn the properties into a Json object
        Json config = Json.make(properties);

        // Remove the JWT Secret
        if(config.has(GraknEngineConfig.JWT_SECRET_PROPERTY)) {
            config.delAt(GraknEngineConfig.JWT_SECRET_PROPERTY);
        }

        return config.toString();
    }

    @GET
    @Path("/keyspaces")
    @ApiOperation(value = "Get all the key spaces that have been opened")
    private String getKeyspaces(Request request, Response response) {
        try (GraknGraph graph = factory.getGraph(SystemKeyspace.SYSTEM_GRAPH_NAME, GraknTxType.WRITE)) {
            ResourceType<String> keyspaceName = graph.getType(SystemKeyspace.KEYSPACE_RESOURCE);
            Json result = Json.array();
            if (graph.getType(SystemKeyspace.KEYSPACE_ENTITY) == null) {
                LOG.warn("No system ontology in system keyspace, possibly a bug!");
                return result.toString();
            }
            for (Entity keyspace : graph.<EntityType>getType(SystemKeyspace.KEYSPACE_ENTITY).instances()) {
                Collection<Resource<?>> names = keyspace.resources(keyspaceName);
                if (names.size() != 1) {
                    throw GraknServerException.internalError(ErrorMessage.INVALID_SYSTEM_KEYSPACE.getMessage(" keyspace " + keyspace.getId() + " has no unique name."));
                }
                result.add(names.iterator().next().getValue());
            }
            return result.toString();
        } catch (Exception e) {
            LOG.error("While retrieving keyspace list:", e);
            throw GraknServerException.serverException(500, e);
        }
    }

    @GET
    @Path("/metrics")
    @ApiOperation(value = "Exposes internal metrics")
    private String getMetrics(Request request, Response response) throws IOException {
        response.type(APPLICATION_JSON);
        response.header("Cache-Control", "must-revalidate,no-cache,no-store");
        response.status(HttpServletResponse.SC_OK);
        if (request.queryParamOrDefault("format", "default").equals("prometheus")) {
            final Writer writer1 = new StringWriter();
            TextFormat.write004(writer1, this.prometheusRegistry.metricFamilySamples());
            return writer1.toString();
        } else {
            final ObjectWriter writer = mapper.writer();
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                writer.writeValue(output, this.metricRegistry);
                return new String(output.toByteArray(), "UTF-8");
            }
        }
    }

}

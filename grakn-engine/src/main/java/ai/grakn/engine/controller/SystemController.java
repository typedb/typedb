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


import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.Keyspace;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.EntityType;
import ai.grakn.GraknConfigKey;
import ai.grakn.engine.GraknEngineStatus;
import ai.grakn.engine.SystemKeyspace;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.ErrorMessage;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import mjson.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ai.grakn.util.REST.Request.FORMAT;
import static ai.grakn.util.REST.Request.KEYSPACE;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static ai.grakn.util.REST.WebPath.System.CONFIGURATION;
import static ai.grakn.util.REST.WebPath.System.DELETE_KEYSPACE;
import static ai.grakn.util.REST.WebPath.System.INITIALISE;
import static ai.grakn.util.REST.WebPath.System.KEYSPACES;
import static ai.grakn.util.REST.WebPath.System.METRICS;
import static ai.grakn.util.REST.WebPath.System.STATUS;
import static org.apache.http.HttpHeaders.CACHE_CONTROL;


/**
 * <p> Controller Providing Configs for building Grakn Graphs </p>
 *
 * <p> When calling {@link ai.grakn.Grakn#session(String, String)} and using the non memory location
 * this controller is accessed. The controller provides the necessary config needed in order to
 * build a {@link GraknTx}.
 *
 * This controller also allows the retrieval of all keyspaces opened so far. </p>
 *
 * @author fppt
 */
public class SystemController {

    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4";
    private static final String PROMETHEUS = "prometheus";
    private static final String JSON = "json";

    private final Logger LOG = LoggerFactory.getLogger(SystemController.class);
    private final EngineGraknTxFactory factory;
    private final GraknEngineStatus graknEngineStatus;
    private final MetricRegistry metricRegistry;
    private final ObjectMapper mapper;
    private final CollectorRegistry prometheusRegistry;

    public SystemController(EngineGraknTxFactory factory, Service spark,
            GraknEngineStatus graknEngineStatus, MetricRegistry metricRegistry) {
        this.factory = factory;
        this.graknEngineStatus = graknEngineStatus;
        this.metricRegistry = metricRegistry;
        DropwizardExports prometheusMetricWrapper = new DropwizardExports(metricRegistry);
        this.prometheusRegistry = new CollectorRegistry();
        prometheusRegistry.register(prometheusMetricWrapper);
        spark.get(KEYSPACES, this::getKeyspaces);
        spark.get(ai.grakn.util.REST.WebPath.System.KEYSPACE, this::getKeyspace);
        spark.get(CONFIGURATION, this::getConfiguration);
        spark.get(METRICS, this::getMetrics);
        spark.get(INITIALISE, this::initialiseSession);
        spark.get(STATUS, this::getStatus);
        spark.delete(DELETE_KEYSPACE, this::deleteKeyspace);

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
    @Path("/initialise")
    @ApiOperation(value = "Initialise a grakn session - add the keyspace to the system graph and return configured properties.")
    @ApiImplicitParam(name = KEYSPACE, value = "Name of graph to use", required = true, dataType = "string", paramType = "query")
    private String initialiseSession(Request request, Response response) {
        Keyspace keyspace = Keyspace.of(request.queryParams(KEYSPACE_PARAM));
        boolean keyspaceInitialised = factory.systemKeyspace().ensureKeyspaceInitialised(keyspace);

        if (keyspaceInitialised) {
            return getConfiguration(request, response);
        }

        throw GraknServerException
                .internalError("Unable to instantiate system keyspace " + keyspace);
    }

    @DELETE
    @Path("/deleteKeyspace")
    @ApiOperation(value = "Delete a keyspace from the system graph.")
    @ApiImplicitParam(name = KEYSPACE, value = "Name of graph to use", required = true, dataType = "string", paramType = "query")
    private boolean deleteKeyspace(Request request, Response response) {
        Keyspace keyspace = Keyspace.of(request.queryParams(KEYSPACE_PARAM));
        boolean deletionComplete = factory.systemKeyspace().deleteKeyspace(keyspace);
        if (deletionComplete) {
            LOG.info("Keyspace {} deleted", keyspace);
            response.status(200);
            return true;
        } else {
            throw GraknServerException.couldNotDelete(keyspace);
        }
    }

    @GET
    @Path("/configuration")
    @ApiOperation(value = "Get config which is used to build transactions")
    private String getConfiguration(Request request, Response response) {

        // Make a copy of the properties object
        Properties properties = new Properties();
        properties.putAll(factory.properties());

        // Turn the properties into a Json object
        Json jsonConfig = Json.make(properties);

        // Remove the JWT Secret
        if (jsonConfig.has(GraknConfigKey.JWT_SECRET.name())) {
            jsonConfig.delAt(GraknConfigKey.JWT_SECRET.name());
        }

        return jsonConfig.toString();
    }

    @GET
    @Path("/status")
    @ApiOperation(value = "Return the status of the engine: READY, INITIALIZING")
    private String getStatus(Request request, Response response) {
        return graknEngineStatus.isReady() ? "READY" : "INITIALIZING";
    }

    @GET
    @Path("/keyspaces")
    @ApiOperation(value = "Get all the key spaces that have been opened")
    private String getKeyspaces(Request request, Response response) {
        try (GraknTx graph = factory.tx(SystemKeyspace.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {

            AttributeType<String> keyspaceName = graph
                    .getSchemaConcept(SystemKeyspace.KEYSPACE_RESOURCE);
            Json result = Json.array();

            graph.<EntityType>getSchemaConcept(SystemKeyspace.KEYSPACE_ENTITY).instances()
                    .forEach(keyspace -> {
                        Collection<Attribute<?>> names = keyspace.attributes(keyspaceName)
                                .collect(Collectors.toSet());
                        if (names.size() != 1) {
                            throw GraknServerException.internalError(
                                    ErrorMessage.INVALID_SYSTEM_KEYSPACE.getMessage(
                                            " keyspace " + keyspace.getId()
                                                    + " has no unique name."));
                        }
                        result.add(names.iterator().next().getValue());
                    });
            return result.toString();
        } catch (Exception e) {
            LOG.error("While retrieving getKeyspace list:", e);
            throw GraknServerException.serverException(500, e);
        }
    }

    @GET
    @Path("/keyspaces/{keyspace}")
    @ApiOperation(value = "Get the keyspace provided")
    private String getKeyspace(Request request, Response response) {
        String keyspace = request.params(":keyspace");
        if (keyspace == null || keyspace.isEmpty()) {
            throw GraknServerException.requestMissingParameters("keyspace");
        }
        try (GraknTx graph = factory.tx(SystemKeyspace.SYSTEM_KB_KEYSPACE, GraknTxType.WRITE)) {
            AttributeType<String> keyspaceName = graph
                    .getSchemaConcept(SystemKeyspace.KEYSPACE_RESOURCE);
            boolean exists = graph.<EntityType>getSchemaConcept(SystemKeyspace.KEYSPACE_ENTITY)
                    .instances().anyMatch(k -> k.attributes(keyspaceName).map(Attribute::getValue).collect(Collectors.toSet()).contains(keyspace));
            if (exists) {
                Json result = Json.object("value", keyspace);
                return result.toString();
            } else {
                throw GraknBackendException.noSuchKeyspace(Keyspace.of(keyspace));
            }
        }
    }

    @GET
    @Path("/metrics")
    @ApiOperation(value = "Exposes internal metrics")
    @ApiImplicitParams({
            @ApiImplicitParam(name = FORMAT, value = "prometheus", dataType = "string", paramType = "path")
    })
    private String getMetrics(Request request, Response response) throws IOException {
        response.header(CACHE_CONTROL, "must-revalidate,no-cache,no-store");
        response.status(HttpServletResponse.SC_OK);
        Optional<String> format = Optional.ofNullable(request.queryParams(FORMAT));
        String dFormat = format.orElse(JSON);
        if (dFormat.equals(PROMETHEUS)) {
            // Prometheus format for the metrics
            response.type(PROMETHEUS_CONTENT_TYPE);
            final Writer writer1 = new StringWriter();
            TextFormat.write004(writer1, this.prometheusRegistry.metricFamilySamples());
            return writer1.toString();
        } else if (dFormat.equals(JSON)) {
            // Json/Dropwizard format
            response.type(APPLICATION_JSON);
            final ObjectWriter writer = mapper.writer();
            try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                writer.writeValue(output, this.metricRegistry);
                return new String(output.toByteArray(), "UTF-8");
            }
        } else {
            throw new IllegalArgumentException("Unexpected format " + dFormat);
        }
    }

}

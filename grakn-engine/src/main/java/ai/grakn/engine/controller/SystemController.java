/*
 * Grakn - A Distributed Semantic Database
 * Copyright (C) 2016-2018 Grakn Labs Limited
 *
 * Grakn is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Grakn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.controller;


import ai.grakn.GraknConfigKey;
import ai.grakn.GraknTx;
import ai.grakn.engine.GraknConfig;
import ai.grakn.engine.KeyspaceStore;
import ai.grakn.engine.ServerStatus;
import ai.grakn.engine.controller.response.Keyspace;
import ai.grakn.engine.controller.response.Keyspaces;
import ai.grakn.engine.controller.response.Root;
import ai.grakn.engine.controller.util.Requests;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.GraknVersion;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.commons.io.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ai.grakn.util.REST.Request.FORMAT;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Response.ContentType.APPLICATION_JSON;
import static org.apache.http.HttpHeaders.CACHE_CONTROL;


/**
 * <p> Controller Providing Configs for building Grakn Graphs </p>
 *
 * <p> When calling {@link ai.grakn.Grakn#session(String, String)} and using the non memory location
 * this controller is accessed. The controller provides the necessary config needed in order to
 * build a {@link GraknTx}.
 *
 * This controller also allows the retrieval of all {@link ai.grakn.Keyspace}s opened so far. </p>
 *
 * @author Filipe Peliz Pinto Teixeira
 */
public class SystemController implements HttpController {

    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4";
    private static final String PROMETHEUS = "prometheus";
    private static final String JSON = "json";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Logger LOG = LoggerFactory.getLogger(SystemController.class);
    private final ServerStatus serverStatus;
    private final MetricRegistry metricRegistry;
    private final ObjectMapper mapper;
    private final CollectorRegistry prometheusRegistry;
    private final KeyspaceStore keyspaceStore;
    private final GraknConfig config;

    public SystemController(GraknConfig config, KeyspaceStore keyspaceStore,
                            ServerStatus serverStatus, MetricRegistry metricRegistry) {
        this.keyspaceStore = keyspaceStore;
        this.config = config;

        this.serverStatus = serverStatus;
        this.metricRegistry = metricRegistry;
        DropwizardExports prometheusMetricWrapper = new DropwizardExports(metricRegistry);
        this.prometheusRegistry = new CollectorRegistry();
        prometheusRegistry.register(prometheusMetricWrapper);

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

    @Override
    public void start(Service spark) {

        spark.get(REST.WebPath.ROOT, this::getRoot);

        spark.get(REST.WebPath.KB, (req, res) -> getKeyspaces(res));
        spark.get(REST.WebPath.KB_KEYSPACE, this::getKeyspace);
        spark.put(REST.WebPath.KB_KEYSPACE, this::putKeyspace);
        spark.delete(REST.WebPath.KB_KEYSPACE, this::deleteKeyspace);
        spark.get(REST.WebPath.METRICS, this::getMetrics);
        spark.get(REST.WebPath.STATUS, (req, res) -> getStatus());
        spark.get(REST.WebPath.VERSION, (req, res) -> getVersion());
    }

    @GET
    @Path(REST.WebPath.ROOT)
    private String getRoot(Request request, Response response) throws JsonProcessingException {
        // Handle root here for JSON, otherwise redirect to HTML page
        if (Requests.getAcceptType(request).equals(APPLICATION_JSON)) {
            return getJsonRoot(response);
        } else {
            return getIndexPage();
        }
    }

    private String getJsonRoot(Response response) throws JsonProcessingException {
        response.type(APPLICATION_JSON);
        Root root = Root.create();
        return objectMapper.writeValueAsString(root);
    }

    private String getIndexPage() {
        try {
            return new String(Files.readAllBytes(dashboardHtml()), Charsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private java.nio.file.Path dashboardHtml() {
        return config.getPath(GraknConfigKey.STATIC_FILES_PATH).resolve("dashboard.html");
    }

    @GET
    @Path(REST.WebPath.VERSION)
    private String getVersion() {
        return GraknVersion.VERSION;
    }

    @GET
    @Path(REST.WebPath.KB)
    private String getKeyspaces(Response response) throws JsonProcessingException {
        response.type(APPLICATION_JSON);
        Set<Keyspace> keyspaces = keyspaceStore.keyspaces().stream().
                map(Keyspace::of).
                collect(Collectors.toSet());
        return objectMapper.writeValueAsString(Keyspaces.of(keyspaces));
    }

    @GET
    @Path("/kb/{keyspace}")
    private String getKeyspace(Request request, Response response) throws JsonProcessingException {
        response.type(APPLICATION_JSON);
        ai.grakn.Keyspace keyspace = ai.grakn.Keyspace.of(Requests.mandatoryPathParameter(request, KEYSPACE_PARAM));

        if (keyspaceStore.containsKeyspace(keyspace)) {
            response.status(HttpServletResponse.SC_OK);
            return objectMapper.writeValueAsString(Keyspace.of(keyspace));
        } else {
            response.status(HttpServletResponse.SC_NOT_FOUND);
            return "";
        }
    }

    @PUT
    @Path("/kb/{keyspace}")
    private String putKeyspace(Request request, Response response) throws JsonProcessingException {
        ai.grakn.Keyspace keyspace = ai.grakn.Keyspace.of(Requests.mandatoryPathParameter(request, KEYSPACE_PARAM));
        keyspaceStore.addKeyspace(keyspace);
        response.status(HttpServletResponse.SC_OK);
        response.type(APPLICATION_JSON);
        return objectMapper.writeValueAsString(config);
    }

    @DELETE
    @Path("/kb/{keyspace}")
    private boolean deleteKeyspace(Request request, Response response) {
        ai.grakn.Keyspace keyspace = ai.grakn.Keyspace.of(Requests.mandatoryPathParameter(request, KEYSPACE_PARAM));
        boolean deletionComplete = keyspaceStore.deleteKeyspace(keyspace);
        if (deletionComplete) {
            LOG.info("Keyspace {} deleted", keyspace);
            response.status(HttpServletResponse.SC_NO_CONTENT);
            return true;
        } else {
            throw GraknServerException.couldNotDelete(keyspace);
        }
    }

    @GET
    @Path("/status")
    private String getStatus() {
        return serverStatus.isReady() ? "READY" : "INITIALIZING";
    }

    @GET
    @Path("/metrics")
    private String getMetrics(Request request, Response response) throws IOException {
        response.header(CACHE_CONTROL, "must-revalidate,no-cache,no-store");
        response.status(HttpServletResponse.SC_OK);
        Optional<String> format = Optional.ofNullable(request.queryParams(FORMAT));
        String dFormat = format.orElse(JSON);
        switch (dFormat) {
            case PROMETHEUS:
                // Prometheus format for the metrics
                response.type(PROMETHEUS_CONTENT_TYPE);
                final Writer writer1 = new StringWriter();
                TextFormat.write004(writer1, this.prometheusRegistry.metricFamilySamples());
                return writer1.toString();
            case JSON:
                // Json/Dropwizard format
                response.type(APPLICATION_JSON);
                final ObjectWriter writer = mapper.writer();
                try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                    writer.writeValue(output, this.metricRegistry);
                    return new String(output.toByteArray(), "UTF-8");
                }
            default:
                throw GraknServerException.requestInvalidParameter(FORMAT, dFormat);
        }
    }
}

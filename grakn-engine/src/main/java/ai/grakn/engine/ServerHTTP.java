/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2018 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ai.grakn.engine;


import ai.grakn.GraknConfigKey;
import ai.grakn.engine.attribute.uniqueness.AttributeUniqueness;
import ai.grakn.engine.controller.ConceptController;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.controller.HttpController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.printer.JacksonPrinter;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknServerException;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Response;
import spark.Service;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

/**
 * @author Michele Orsi
 */
public class ServerHTTP {

    private static final Logger LOG = LoggerFactory.getLogger(ServerHTTP.class);

    private final GraknConfig prop;
    private final Service spark;
    private final EngineGraknTxFactory factory;
    private final MetricRegistry metricRegistry;
    private final ServerStatus serverStatus;
    private final AttributeUniqueness attributeUniqueness;
    private final ServerRPC rpcServerRPC;
    private final Collection<HttpController> additionalCollaborators;

    public ServerHTTP(
            GraknConfig prop, Service spark, EngineGraknTxFactory factory, MetricRegistry metricRegistry,
            ServerStatus serverStatus, AttributeUniqueness attributeUniqueness,
            ServerRPC rpcServerRPC,
            Collection<HttpController> additionalCollaborators
    ) {
        this.prop = prop;
        this.spark = spark;
        this.factory = factory;
        this.metricRegistry = metricRegistry;
        this.serverStatus = serverStatus;
        this.attributeUniqueness = attributeUniqueness;
        this.rpcServerRPC = rpcServerRPC;
        this.additionalCollaborators = additionalCollaborators;
    }


    public void startHTTP() throws IOException {
        configureSpark(spark, prop);

        startCollaborators();

        rpcServerRPC.start();
        // This method will block until all the controllers are ready to serve requests
        spark.awaitInitialization();
    }

    protected void startCollaborators() {
        JacksonPrinter printer = JacksonPrinter.create();

        // Start all the DEFAULT controllers
        new GraqlController(factory, attributeUniqueness, printer, metricRegistry).start(spark);
        new ConceptController(factory, metricRegistry).start(spark);
        new SystemController(prop, factory.keyspaceStore(), serverStatus, metricRegistry).start(spark);

        additionalCollaborators.forEach(httpController -> httpController.start(spark));
    }

    public static void configureSpark(Service spark, GraknConfig prop) {
        configureSpark(spark,
                prop.getProperty(GraknConfigKey.SERVER_HOST_NAME),
                prop.getProperty(GraknConfigKey.SERVER_PORT),
                prop.getPath(GraknConfigKey.STATIC_FILES_PATH),
                prop.getProperty(GraknConfigKey.WEBSERVER_THREADS));
    }

    public static void configureSpark(Service spark,
                                      String hostName,
                                      int port,
                                      Path staticFolder,
                                      int maxThreads) {
        // Set host name
        spark.ipAddress(hostName);

        // Set port
        spark.port(port);

        // Set the external static files folder
        spark.staticFiles.externalLocation(staticFolder.toString());

        spark.threadPool(maxThreads);

        //Register exception handlers
        spark.exception(GraknServerException.class, (e, req, res) -> {
            assert e instanceof GraknServerException; // This is guaranteed by `spark#exception`
            handleGraknServerError((GraknServerException) e, res);
        });

        spark.exception(Exception.class, (e, req, res) -> handleInternalError(e, res));
    }


    public void stopHTTP() throws InterruptedException {
        rpcServerRPC.close();

        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            } catch (IllegalStateException e) {
                LOG.debug("Spark server has been stopped");
                running = false;
            }
        }
    }

    /**
     * Handle any {@link GraknBackendException} that are thrown by the server. Configures and returns
     * the correct JSON response.
     *
     * @param exception exception thrown by the server
     * @param response  response to the client
     */
    protected static void handleGraknServerError(GraknServerException exception, Response response) {
        LOG.error("REST error", exception);
        response.status(exception.getStatus());
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * Handle any exception thrown by the server
     *
     * @param exception Exception by the server
     * @param response  response to the client
     */
    protected static void handleInternalError(Exception exception, Response response) {
        LOG.error("REST error", exception);
        response.status(500);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }
}

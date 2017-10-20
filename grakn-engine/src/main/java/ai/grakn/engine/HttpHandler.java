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

package ai.grakn.engine;


import ai.grakn.GraknConfigKey;
import ai.grakn.engine.controller.AuthController;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.ConceptController;
import ai.grakn.engine.controller.DashboardController;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.controller.UserController;
import ai.grakn.engine.controller.api.AttributeController;
import ai.grakn.engine.controller.api.AttributeTypeController;
import ai.grakn.engine.controller.api.EntityController;
import ai.grakn.engine.controller.api.EntityTypeController;
import ai.grakn.engine.controller.api.RelationshipController;
import ai.grakn.engine.controller.api.RelationshipTypeController;
import ai.grakn.engine.controller.api.RoleController;
import ai.grakn.engine.controller.api.RuleController;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.session.RemoteSession;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import mjson.Json;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.HaltException;
import spark.Request;
import spark.Response;
import spark.Service;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static ai.grakn.engine.GraknEngineConfig.WEBSOCKET_TIMEOUT;

/**
 *
 * @author Michele Orsi
 */
public class HttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpHandler.class);

    private final GraknEngineConfig prop;
    private final Service spark;
    private final EngineGraknTxFactory factory;
    private final MetricRegistry metricRegistry;
    private final GraknEngineStatus graknEngineStatus;
    private final TaskManager taskManager;
    private final ExecutorService taskExecutor;

    private static final Set<String> unauthenticatedEndPoints = new HashSet<>(Arrays.asList(
            REST.WebPath.NEW_SESSION_URI,
            REST.WebPath.REMOTE_SHELL_URI,
            REST.WebPath.System.CONFIGURATION,
            REST.WebPath.IS_PASSWORD_PROTECTED_URI));

    public HttpHandler(GraknEngineConfig prop, Service spark, EngineGraknTxFactory factory, MetricRegistry metricRegistry, GraknEngineStatus graknEngineStatus, TaskManager taskManager, ExecutorService taskExecutor) {
        this.prop = prop;
        this.spark = spark;
        this.factory = factory;
        this.metricRegistry = metricRegistry;
        this.graknEngineStatus = graknEngineStatus;
        this.taskManager = taskManager;
        this.taskExecutor = taskExecutor;
    }


    public void startHTTP() {
        boolean passwordProtected = prop.getProperty(GraknConfigKey.PASSWORD_PROTECTED);

        // TODO: Make sure controllers handle the null case
        Optional<String> secret = prop.getProperty(GraknConfigKey.JWT_SECRET);
        @Nullable JWTHandler jwtHandler = secret.map(JWTHandler::create).orElse(null);
        UsersHandler usersHandler = UsersHandler.create(prop.getProperty(GraknConfigKey.ADMIN_PASSWORD), factory);

        configureSpark(spark, prop, jwtHandler);

        // Start the websocket for Graql
        RemoteSession graqlWebSocket = passwordProtected ? RemoteSession.passwordProtected(usersHandler) : RemoteSession.create();
        spark.webSocket(REST.WebPath.REMOTE_SHELL_URI, graqlWebSocket);

        int postProcessingDelay = prop.getProperty(GraknConfigKey.POST_PROCESSING_TASK_DELAY);

        // Start all the controllers
        new GraqlController(factory, spark, metricRegistry);
        new ConceptController(factory, spark, metricRegistry);
        new DashboardController(factory, spark);
        new SystemController(factory, spark, graknEngineStatus, metricRegistry);
        new AuthController(spark, passwordProtected, jwtHandler, usersHandler);
        new UserController(spark, usersHandler);
        new CommitLogController(spark, postProcessingDelay, taskManager);
        new TasksController(spark, taskManager, metricRegistry, taskExecutor);
        new EntityController(factory, spark);
        new EntityTypeController(factory, spark);
        new RelationshipController(factory, spark);
        new RelationshipTypeController(factory, spark);
        new AttributeController(factory, spark);
        new AttributeTypeController(factory, spark);
        new RoleController(factory, spark);
        new RuleController(factory, spark);

        // This method will block until all the controllers are ready to serve requests
        spark.awaitInitialization();
    }

    public static void configureSpark(Service spark, GraknEngineConfig prop, @Nullable JWTHandler jwtHandler) {
        configureSpark(spark,
                prop.getProperty(GraknConfigKey.SERVER_HOST_NAME),
                prop.getProperty(GraknConfigKey.SERVER_PORT),
                prop.getPath(GraknConfigKey.STATIC_FILES_PATH),
                prop.getProperty(GraknConfigKey.PASSWORD_PROTECTED),
                prop.getProperty(GraknConfigKey.WEBSERVER_THREADS),
                jwtHandler);
    }

    public static void configureSpark(Service spark,
                                      String hostName,
                                      int port,
                                      Path staticFolder,
                                      boolean passwordProtected,
                                      int maxThreads,
                                      @Nullable JWTHandler jwtHandler){
        // Set host name
        spark.ipAddress(hostName);

        // Set port
        spark.port(port);

        // Set the external static files folder
        spark.staticFiles.externalLocation(staticFolder.toString());

        spark.threadPool(maxThreads);
        spark.webSocketIdleTimeoutMillis(WEBSOCKET_TIMEOUT);

        // Register filter to check authentication token in each request
        if (passwordProtected) {
            spark.before((req, res) -> checkAuthorization(spark, req, jwtHandler));
        }

        //Register exception handlers
        spark.exception(GraknServerException.class, (e, req, res) -> {
            assert e instanceof GraknServerException; // This is guaranteed by `spark#exception`
            handleGraknServerError((GraknServerException) e, res);
        });

        spark.exception(Exception.class, (e, req, res) -> handleInternalError(e, res));
    }


    public void stopHTTP() {
        spark.stop();

        // Block until server is truly stopped
        // This occurs when there is no longer a port assigned to the Spark server
        boolean running = true;
        while (running) {
            try {
                spark.port();
            }
            catch(IllegalStateException e){
                LOG.debug("Spark server has been stopped");
                running = false;
            }
        }
    }


    /**
     * If authorization is enabled, check the client has correct JWT Token before allowing
     * access to specific endpoints.
     * @param request request information from the client
     */
    private static void checkAuthorization(Service spark, Request request, JWTHandler jwtHandler) throws HaltException {
        //we dont check authorization token if the path requested is one of the unauthenticated ones
        if (!unauthenticatedEndPoints.contains(request.pathInfo())) {
            //add check to see if string contains substring "Bearer ", for now a lot of optimism here
            boolean authenticated;
            try {
                if (request.headers("Authorization") == null || !request.headers("Authorization").startsWith("Bearer ")) {
                    throw GraknServerException.authenticationFailure();
                }

                String token = request.headers("Authorization").substring(7);
                authenticated = jwtHandler.verifyJWT(token);
                request.attribute(REST.Request.USER_ATTR, jwtHandler.extractUserFromJWT(token));
            }
            catch (GraknBackendException e) {
                throw e;
            }
            catch (Exception e) {
                //request is malformed, return 400
                throw GraknServerException.serverException(400, e);
            }
            if (!authenticated) {
                throw spark.halt(401, "User not authenticated.");
            }
        }
    }

    /**
     * Handle any {@link GraknBackendException} that are thrown by the server. Configures and returns
     * the correct JSON response.
     *
     * @param exception exception thrown by the server
     * @param response response to the client
     */
    private static void handleGraknServerError(GraknServerException exception, Response response){
        LOG.error("REST error", exception);
        response.status(exception.getStatus());
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }

    /**
     * Handle any exception thrown by the server
     * @param exception Exception by the server
     * @param response response to the client
     */
    private static void handleInternalError(Exception exception, Response response){
        LOG.error("REST error", exception);
        response.status(500);
        response.body(Json.object("exception", exception.getMessage()).toString());
        response.type(ContentType.APPLICATION_JSON.getMimeType());
    }
}

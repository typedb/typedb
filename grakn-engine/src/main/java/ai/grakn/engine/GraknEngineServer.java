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

import static ai.grakn.engine.GraknEngineConfig.REDIS_SENTINEL_MASTER;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SERVER_PORT;
import static ai.grakn.engine.GraknEngineConfig.REDIS_SERVER_URL;
import static ai.grakn.engine.GraknEngineConfig.WEBSOCKET_TIMEOUT;
import ai.grakn.engine.controller.AuthController;
import ai.grakn.engine.controller.CommitLogController;
import ai.grakn.engine.controller.ConceptController;
import ai.grakn.engine.controller.DashboardController;
import ai.grakn.engine.controller.GraqlController;
import ai.grakn.engine.controller.SystemController;
import ai.grakn.engine.controller.TasksController;
import ai.grakn.engine.controller.UserController;
import ai.grakn.engine.externalcomponents.RedisSupervisor;
import ai.grakn.engine.factory.EngineGraknGraphFactory;
import ai.grakn.engine.lock.ProcessWideLockProvider;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.lock.RedissonLockProvider;
import ai.grakn.engine.session.RemoteSession;
import ai.grakn.engine.externalcomponents.CassandraSupervisor;
import ai.grakn.engine.externalcomponents.OperatingSystemCalls;
import ai.grakn.engine.tasks.connection.RedisCountStorage;
import ai.grakn.engine.tasks.manager.StandaloneTaskManager;
import ai.grakn.engine.tasks.manager.TaskManager;
import ai.grakn.engine.tasks.manager.redisqueue.RedisTaskManager;
import ai.grakn.engine.user.UsersHandler;
import ai.grakn.engine.util.EngineID;
import ai.grakn.engine.util.JWTHandler;
import ai.grakn.exception.GraknBackendException;
import ai.grakn.exception.GraknServerException;
import ai.grakn.util.REST;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import mjson.Json;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;
import org.apache.http.entity.ContentType;
import org.redisson.Redisson;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.util.Pool;
import spark.HaltException;
import spark.Request;
import spark.Response;
import spark.Service;

/**
 * Main class in charge to start a web server and all the REST controllers.
 *
 * @author Marco Scoppetta
 */

public class GraknEngineServer implements AutoCloseable {

    private static final String LOAD_SYSTEM_ONTOLOGY_LOCK_NAME = "load-system-ontology";
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);
    private static final Set<String> unauthenticatedEndPoints = new HashSet<>(Arrays.asList(
            REST.WebPath.NEW_SESSION_URI,
            REST.WebPath.REMOTE_SHELL_URI,
            REST.WebPath.System.CONFIGURATION,
            REST.WebPath.IS_PASSWORD_PROTECTED_URI));

    private final GraknEngineConfig prop;
    private final EngineID engineId = EngineID.me();
    private final Service spark = Service.ignite();
    private final TaskManager taskManager;
    private final EngineGraknGraphFactory factory;
    private final RedisCountStorage redisCountStorage;
    private final MetricRegistry metricRegistry;
    private final Pool<Jedis> jedisPool;
    private final boolean inMemoryQueue;
    private final LockProvider lockProvider;

    public GraknEngineServer(GraknEngineConfig prop) {
        this.prop = prop;
        int redisPort = Integer.parseInt(prop.tryProperty(REDIS_SERVER_PORT).orElse("6379"));
        String redisUrl = prop.tryProperty(REDIS_SERVER_URL).orElse("localhost");
        this.jedisPool = instantiateRedis(prop, redisUrl, redisPort);
        this.redisCountStorage = RedisCountStorage.create(jedisPool);
        String taskManagerClassName = prop.getProperty(GraknEngineConfig.TASK_MANAGER_IMPLEMENTATION);
        this.inMemoryQueue = !taskManagerClassName.contains("RedisTaskManager");
        this.lockProvider = this.inMemoryQueue ? new ProcessWideLockProvider()
                : instantiateRedissonLockProvider(redisPort, redisUrl);
        this.factory = EngineGraknGraphFactory.create(prop.getProperties());
        this.metricRegistry = new MetricRegistry();
        this.taskManager = startTaskManager(inMemoryQueue, redisCountStorage, jedisPool, lockProvider);
    }

    public static void main(String[] args) {
        GraknEngineConfig prop = GraknEngineConfig.create();

        // Start external components (Cassandra and Redis)
        OperatingSystemCalls osCalls = new OperatingSystemCalls();
        CassandraSupervisor cassandraSupervisor = new CassandraSupervisor(prop, osCalls, "");
        RedisSupervisor redisSupervisor = new RedisSupervisor(osCalls, "");

        // Start Grakn Engine
        GraknEngineServer graknEngineServer = new GraknEngineServer(prop);
        graknEngineServer.start();

        // close  on SIGTERM
        Runnable shutdownExternalComponentsAndEngine = () -> {
            try {
                cassandraSupervisor.stopIfRunning();
                redisSupervisor.stopIfRunning();
                graknEngineServer.close();
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownExternalComponentsAndEngine, "GraknEngineServer-shutdown"));
    }

    public void start() {
        lockAndInitializeSystemOntology();
        startHTTP();
        printStartMessage(prop.getProperty(GraknEngineConfig.SERVER_HOST_NAME),
                prop.getProperty(GraknEngineConfig.SERVER_PORT_NUMBER));
    }

    @Override
    public void close() {
        stopHTTP();
        stopTaskManager();
    }

    private void lockAndInitializeSystemOntology() {
        try {
            Lock lock = lockProvider.getLock(LOAD_SYSTEM_ONTOLOGY_LOCK_NAME);
            if (lock.tryLock(60, TimeUnit.SECONDS)) {
                loadAndUnlock(lock);
            } else {
                LOG.info("{} found system ontology lock already acquired by other engine", this.engineId);
            }
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted while initializing system ontology", this.engineId);
        }
    }

    private void loadAndUnlock(Lock lock) {
        try {
            LOG.info("{} is initializing the system ontology", this.engineId);
            factory.systemKeyspace().loadSystemOntology();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check in with the properties file to decide which type of task manager should be started
     * @param inMemoryQueue
     * @param redisCountStorage
     * @param jedisPool
     */
    private TaskManager startTaskManager(
            boolean inMemoryQueue,
            RedisCountStorage redisCountStorage,
            Pool<Jedis> jedisPool, LockProvider lockProvider) {
        TaskManager taskManager;
        if (!inMemoryQueue) {
            taskManager = new RedisTaskManager(engineId, prop, jedisPool, factory, lockProvider, metricRegistry);
        } else  {
            taskManager = new StandaloneTaskManager(engineId, prop, redisCountStorage, factory, lockProvider, metricRegistry);
        }
        taskManager.start();
        return taskManager;
    }

    public void startHTTP() {
        boolean passwordProtected = prop.getPropertyAsBool(GraknEngineConfig.PASSWORD_PROTECTED_PROPERTY, false);

        // TODO: Make sure controllers handle the null case
        Optional<String> secret = prop.tryProperty(GraknEngineConfig.JWT_SECRET_PROPERTY);
        @Nullable JWTHandler jwtHandler = secret.map(JWTHandler::create).orElse(null);
        UsersHandler usersHandler = UsersHandler.create(prop.getProperty(GraknEngineConfig.ADMIN_PASSWORD_PROPERTY), factory);

        configureSpark(spark, prop, jwtHandler);

        // Start the websocket for Graql
        RemoteSession graqlWebSocket = passwordProtected ? RemoteSession.passwordProtected(usersHandler) : RemoteSession.create();
        spark.webSocket(REST.WebPath.REMOTE_SHELL_URI, graqlWebSocket);

        String defaultKeyspace = prop.getProperty(GraknEngineConfig.DEFAULT_KEYSPACE_PROPERTY);
        int postProcessingDelay = prop.getPropertyAsInt(GraknEngineConfig.POST_PROCESSING_TASK_DELAY);

        // Start all the controllers
        new GraqlController(factory, spark, metricRegistry);
        new ConceptController(factory, spark, metricRegistry);
        new DashboardController(factory, spark);
        new SystemController(factory, spark, metricRegistry);
        new AuthController(spark, passwordProtected, jwtHandler, usersHandler);
        new UserController(spark, usersHandler);
        new CommitLogController(spark, defaultKeyspace, postProcessingDelay, taskManager);
        new TasksController(spark, taskManager, metricRegistry);

        // This method will block until all the controllers are ready to serve requests
        spark.awaitInitialization();
    }

    public static void configureSpark(Service spark, GraknEngineConfig prop, @Nullable JWTHandler jwtHandler) {
        configureSpark(spark, 
                       prop.getProperty(GraknEngineConfig.SERVER_HOST_NAME),
                       Integer.parseInt(prop.getProperty(GraknEngineConfig.SERVER_PORT_NUMBER)),
                       prop.getPath(GraknEngineConfig.STATIC_FILES_PATH),
                       prop.getPropertyAsBool(GraknEngineConfig.PASSWORD_PROTECTED_PROPERTY, false),
                       jwtHandler);
    }
    
    public static void configureSpark(Service spark, 
                                      String hostName, 
                                      int port, 
                                      String staticFolder,
                                      boolean passwordProtected,
                                      @Nullable JWTHandler jwtHandler){
        // Set host name
        spark.ipAddress(hostName);

        // Set port
        spark.port(port);

        // Set the external static files folder
        spark.staticFiles.externalLocation(staticFolder);

        spark.webSocketIdleTimeoutMillis(WEBSOCKET_TIMEOUT);

        //Register filter to check authentication token in each request
        boolean isPasswordProtected = passwordProtected;

        if (isPasswordProtected) {
            spark.before((req, res) -> checkAuthorization(spark, req, jwtHandler));
        }

        //Register exception handlers
        spark.exception(GraknBackendException.class, (e, req, res) -> handleGraknServerError(e, res));
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

    private void stopTaskManager() {
        try {
            taskManager.close();
        } catch (Exception e){
            LOG.error(getFullStackTrace(e));
        }
    }

    public TaskManager getTaskManager(){
        return taskManager;
    }

    public EngineGraknGraphFactory factory() {
        return factory;
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
                spark.halt(401, "User not authenticated.");
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
    private static void handleGraknServerError(Exception exception, Response response){
        LOG.error("REST error", exception);
        response.status(((GraknServerException) exception).getStatus());
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

    private Pool<Jedis> instantiateRedis(GraknEngineConfig prop, String redisUrl, int redisPort) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        // TODO Make this configurable in property file
        poolConfig.setMaxTotal(128);
        Optional<String> sentinelMaster = prop.tryProperty(REDIS_SENTINEL_MASTER);
        // If sentinel is configured use a sentinel pool
        // TODO Sentinel not fully supported yet
        return sentinelMaster
                .<Pool<Jedis>>map(s -> new JedisSentinelPool(s, ImmutableSet.of(String.format("%s:%s", redisUrl, redisPort)), poolConfig))
                .orElseGet(() -> new JedisPool(poolConfig, redisUrl, redisPort));
    }

    private RedissonLockProvider instantiateRedissonLockProvider(int redisPort, String redisUrl) {
        LOG.info("Connecting redisCountStorage client to {}:{}", redisUrl, redisPort);
        Config redissonConfig = new Config();
        redissonConfig.useSingleServer()
                .setAddress(String.format("%s:%d", redisUrl, redisPort))
                .setConnectionPoolSize(5);
        return new RedissonLockProvider(Redisson.create(redissonConfig));
    }

    private static void printStartMessage(String host, String port) {
        String address = "http://" + host + ":" + port;
        LOG.info("\n==================================================");
        LOG.info("\n" + String.format(GraknEngineConfig.GRAKN_ASCII, address));
        LOG.info("\n==================================================");
    }
}

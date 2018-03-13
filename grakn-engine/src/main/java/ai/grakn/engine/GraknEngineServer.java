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
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/gpl.txt>.
 */
package ai.grakn.engine;

import ai.grakn.GraknConfigKey;
import ai.grakn.engine.data.RedisWrapper;
import ai.grakn.engine.factory.EngineGraknTxFactory;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.task.BackgroundTaskRunner;
import ai.grakn.engine.util.EngineID;
import ai.grakn.util.GraknVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static ai.grakn.util.ErrorMessage.VERSION_MISMATCH;
import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Main class in charge to start a web server and all the REST controllers.
 *
 * @author Marco Scoppetta
 */
public class GraknEngineServer implements AutoCloseable {

    private static final String REDIS_VERSION_KEY = "info:version";

    private static final String LOAD_SYSTEM_SCHEMA_LOCK_NAME = "load-system-schema";
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);

    private final GraknConfig prop;
    private final EngineGraknTxFactory factory;
    private final LockProvider lockProvider;
    private final GraknEngineStatus graknEngineStatus;
    private final RedisWrapper redisWrapper;
    private final HttpHandler httpHandler;
    private final EngineID engineId;
    private final BackgroundTaskRunner backgroundTaskRunner;

    public GraknEngineServer(GraknConfig prop, EngineGraknTxFactory factory, LockProvider lockProvider, GraknEngineStatus graknEngineStatus, RedisWrapper redisWrapper, HttpHandler httpHandler, EngineID engineId, BackgroundTaskRunner backgroundTaskRunner) {
        this.prop = prop;
        this.graknEngineStatus = graknEngineStatus;
        // Redis connection pool
        this.redisWrapper = redisWrapper;
        // Lock provider
        this.lockProvider = lockProvider;
        this.factory = factory;
        this.httpHandler = httpHandler;
        this.engineId = engineId;
        this.backgroundTaskRunner = backgroundTaskRunner;
    }

    public void start() throws IOException {
        redisWrapper.testConnection();
        Stopwatch timer = Stopwatch.createStarted();
        logStartMessage(
                prop.getProperty(GraknConfigKey.SERVER_HOST_NAME),
                prop.getProperty(GraknConfigKey.SERVER_PORT));
        synchronized (this){
            checkVersion();
            lockAndInitializeSystemSchema();
            httpHandler.startHTTP();
        }
        graknEngineStatus.setReady(true);
        LOG.info("Grakn started in {}", timer.stop());
    }

    private void checkVersion() {
        Jedis jedis = redisWrapper.getJedisPool().getResource();
        String storedVersion = jedis.get(REDIS_VERSION_KEY);
        if (storedVersion == null) {
            jedis.set(REDIS_VERSION_KEY, GraknVersion.VERSION);
        } else if (!storedVersion.equals(GraknVersion.VERSION)) {
            LOG.warn(VERSION_MISMATCH.getMessage(GraknVersion.VERSION, storedVersion));
        }
    }

    @VisibleForTesting
    public BackgroundTaskRunner backgroundTaskRunner(){
        return backgroundTaskRunner;
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                httpHandler.stopHTTP();
            } catch (InterruptedException e){
                LOG.error(getFullStackTrace(e));
            }
            redisWrapper.close();
            backgroundTaskRunner.close();
        }
    }

    private void lockAndInitializeSystemSchema() {
        try {
            Lock lock = lockProvider.getLock(LOAD_SYSTEM_SCHEMA_LOCK_NAME);
            if (lock.tryLock(60, TimeUnit.SECONDS)) {
                loadAndUnlock(lock);
            } else {
                LOG.info("{} found system schema lock already acquired by other engine", this.engineId);
            }
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted while initializing system schema", this.engineId);
        }
    }

    private void loadAndUnlock(Lock lock) {
        try {
            LOG.info("{} is checking the system schema", this.engineId);
            factory.systemKeyspace().loadSystemSchema();
        } finally {
            lock.unlock();
        }
    }

    private void logStartMessage(String host, int port) {
        String address = "http://" + host + ":" + port;
        LOG.info("\n==================================================");
        LOG.info("\n" + String.format(GraknConfig.GRAKN_ASCII, address));
        LOG.info("\n==================================================");
    }

    public EngineGraknTxFactory factory() {
        return factory;
    }

    public HttpHandler getHttpHandler() {
        return httpHandler;
    }

    public LockProvider lockProvider(){
        return lockProvider;
    }
}
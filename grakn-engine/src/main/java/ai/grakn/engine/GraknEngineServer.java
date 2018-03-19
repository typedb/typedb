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
import ai.grakn.engine.data.QueueSanityCheck;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.task.BackgroundTaskRunner;
import ai.grakn.engine.util.EngineID;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace;

/**
 * Main class in charge to start a web server and all the REST controllers.
 *
 * @author Marco Scoppetta
 */
public class GraknEngineServer implements AutoCloseable {
    private static final String LOAD_SYSTEM_SCHEMA_LOCK_NAME = "load-system-schema";
    private static final Logger LOG = LoggerFactory.getLogger(GraknEngineServer.class);

    private final EngineID engineId;
    private final GraknConfig config;
    private final GraknEngineStatus graknEngineStatus;
    private final LockProvider lockProvider;
    private final QueueSanityCheck queueSanityCheck;
    private final HttpHandler httpHandler;
    private final BackgroundTaskRunner backgroundTaskRunner;

    private final GraknKeyspaceStore graknKeyspaceStore;

    public GraknEngineServer(EngineID engineId, GraknConfig config, GraknEngineStatus graknEngineStatus, LockProvider lockProvider, QueueSanityCheck queueSanityCheck, HttpHandler httpHandler, BackgroundTaskRunner backgroundTaskRunner, GraknKeyspaceStore graknKeyspaceStore) {
        this.config = config;
        this.graknEngineStatus = graknEngineStatus;
        // Redis connection pool
        this.queueSanityCheck = queueSanityCheck;
        // Lock provider
        this.lockProvider = lockProvider;
        this.graknKeyspaceStore = graknKeyspaceStore;
        this.httpHandler = httpHandler;
        this.engineId = engineId;
        this.backgroundTaskRunner = backgroundTaskRunner;
    }

    public void start() throws IOException {
        queueSanityCheck.testConnection();
        Stopwatch timer = Stopwatch.createStarted();
        logStartMessage(
                config.getProperty(GraknConfigKey.SERVER_HOST_NAME),
                config.getProperty(GraknConfigKey.SERVER_PORT));
        synchronized (this){
            queueSanityCheck.checkVersion();
            lockAndInitializeSystemSchema();
            httpHandler.startHTTP();
        }
        graknEngineStatus.setReady(true);
        LOG.info("Grakn started in {}", timer.stop());
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
            queueSanityCheck.close();
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
            graknKeyspaceStore.loadSystemSchema();
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

    public HttpHandler getHttpHandler() {
        return httpHandler;
    }

    public LockProvider lockProvider(){
        return lockProvider;
    }

    public GraknKeyspaceStore systemKeyspace() { return graknKeyspaceStore; }
}


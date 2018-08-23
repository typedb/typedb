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
import ai.grakn.engine.attribute.uniqueness.AttributeDeduplicatorDaemon;
import ai.grakn.engine.lock.LockProvider;
import ai.grakn.engine.util.EngineID;
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
public class Server implements AutoCloseable {
    private static final String LOAD_SYSTEM_SCHEMA_LOCK_NAME = "load-system-schema";
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private final EngineID engineId;
    private final GraknConfig config;
    private final ServerStatus serverStatus;
    private final LockProvider lockProvider;
    private final ServerHTTP httpHandler;
    private final AttributeDeduplicatorDaemon attributeDeduplicatorDaemon;

    private final KeyspaceStore keyspaceStore;

    public Server(EngineID engineId, GraknConfig config, ServerStatus serverStatus, LockProvider lockProvider, ServerHTTP httpHandler, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore) {
        this.config = config;
        this.serverStatus = serverStatus;
        // Redis connection pool
        // Lock provider
        this.lockProvider = lockProvider;
        this.keyspaceStore = keyspaceStore;
        this.httpHandler = httpHandler;
        this.engineId = engineId;
        this.attributeDeduplicatorDaemon = attributeDeduplicatorDaemon;
    }

    public void start() throws IOException {
        Stopwatch timer = Stopwatch.createStarted();
        logStartMessage(
                config.getProperty(GraknConfigKey.SERVER_HOST_NAME),
                config.getProperty(GraknConfigKey.SERVER_PORT));
        synchronized (this){
            lockAndInitializeSystemSchema();
            httpHandler.startHTTP();
        }
        attributeDeduplicatorDaemon.startDeduplicationDaemon();
        serverStatus.setReady(true);
        LOG.info("Grakn started in {}", timer.stop());
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                httpHandler.stopHTTP();
            } catch (InterruptedException e){
                LOG.error(getFullStackTrace(e));
                Thread.currentThread().interrupt();
            }
            attributeDeduplicatorDaemon.stopDeduplicationDaemon();
        }
    }

    private void lockAndInitializeSystemSchema() {
        try {
            Lock lock = lockProvider.getLock(LOAD_SYSTEM_SCHEMA_LOCK_NAME);
            if (lock.tryLock(60, TimeUnit.SECONDS)) {
                try {
                    LOG.info("{} is checking the system schema", this.engineId);
                    keyspaceStore.loadSystemSchema();
                } finally {
                    lock.unlock();
                }
            } else {
                LOG.info("{} found system schema lock already acquired by other engine", this.engineId);
            }
        } catch (InterruptedException e) {
            LOG.warn("{} was interrupted while initializing system schema", this.engineId);
            Thread.currentThread().interrupt();
        }
    }


    private void logStartMessage(String host, int port) {
        String address = "http://" + host + ":" + port;
        LOG.info("\n==================================================");
        LOG.info("\n" + String.format(GraknConfig.GRAKN_ASCII, address));
        LOG.info("\n==================================================");
    }

    public ServerHTTP getHttpHandler() {
        return httpHandler;
    }

    public LockProvider lockProvider(){
        return lockProvider;
    }

}


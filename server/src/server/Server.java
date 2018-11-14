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
package grakn.core.server;

import grakn.core.server.deduplicator.AttributeDeduplicatorDaemon;
import grakn.core.server.util.LockManager;
import grakn.core.server.util.EngineID;
import grakn.core.server.keyspace.KeyspaceStore;
import grakn.core.util.GraknConfig;
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
 */
public class Server implements AutoCloseable {
    private static final String LOAD_SYSTEM_SCHEMA_LOCK_NAME = "load-system-schema";
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);

    private final EngineID engineId;
    private final GraknConfig config;
    private final ServerStatus serverStatus;
    private final LockManager lockManager;
    private final ServerRPC rpcServer;
    private final AttributeDeduplicatorDaemon attributeDeduplicatorDaemon;

    private final KeyspaceStore keyspaceStore;

    public Server(EngineID engineId, GraknConfig config, ServerStatus serverStatus, LockManager lockManager, ServerRPC rpcServer, AttributeDeduplicatorDaemon attributeDeduplicatorDaemon, KeyspaceStore keyspaceStore) {
        this.config = config;
        this.serverStatus = serverStatus;
        // Redis connection pool
        // Lock provider
        this.lockManager = lockManager;
        this.keyspaceStore = keyspaceStore;
        this.rpcServer = rpcServer;
        this.engineId = engineId;
        this.attributeDeduplicatorDaemon = attributeDeduplicatorDaemon;
    }

    public void start() throws IOException {
        Stopwatch timer = Stopwatch.createStarted();
        printGraknASCII();
        synchronized (this){
            lockAndInitializeSystemSchema();
            rpcServer.start();
        }
        attributeDeduplicatorDaemon.startDeduplicationDaemon();
        serverStatus.setReady(true);
        LOG.info("Grakn started in {}", timer.stop());
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                rpcServer.close();
            } catch (InterruptedException e){
                LOG.error(getFullStackTrace(e)); //TODO: remove commons-lang dependency
                Thread.currentThread().interrupt();
            }
            attributeDeduplicatorDaemon.stopDeduplicationDaemon();
        }
    }

    private void lockAndInitializeSystemSchema() {
        try {
            Lock lock = lockManager.getLock(LOAD_SYSTEM_SCHEMA_LOCK_NAME);
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


    private void printGraknASCII() {
        LOG.info("\n==================================================");
        LOG.info("\n" + GraknConfig.GRAKN_ASCII);
        LOG.info("\n==================================================");
    }
}


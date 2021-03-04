/*
 * Copyright (C) 2021 Grakn Labs
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

package grakn.core.server.database.session;

import com.google.protobuf.ByteString;
import grakn.core.Grakn;
import grakn.core.common.parameters.Options;
import grakn.core.server.GraknService;
import grakn.core.server.database.transaction.TransactionService;
import grakn.core.server.database.transaction.TransactionStream;
import grakn.protocol.TransactionProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.protobuf.ByteString.copyFrom;
import static grakn.core.common.collection.Bytes.uuidToBytes;
import static grakn.core.concurrent.common.Executors.scheduled;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SessionService {

    private static final Logger LOG = LoggerFactory.getLogger(SessionService.class);

    private final ConcurrentHashMap<Integer, TransactionService> transactionServices;
    private final GraknService graknSrv;
    private final Options.Session options;
    private final Grakn.Session session;
    private final AtomicBoolean isOpen;
    private final long idleTimeoutMillis;
    private ScheduledFuture<?> idleTimeoutTask;

    public SessionService(GraknService graknSrv, Grakn.Session session, Options.Session options) {
        this.graknSrv = graknSrv;
        this.session = session;
        this.options = options;
        this.isOpen = new AtomicBoolean(true);
        this.transactionServices = new ConcurrentHashMap<>();
        this.idleTimeoutMillis = options.sessionIdleTimeoutMillis();
        setIdleTimeout();
    }

    public TransactionService transaction(TransactionStream transactionStream,
                                          TransactionProto.Transaction.Open.Req request) {
        TransactionService transactionSrv = new TransactionService(this, transactionStream, request);
        transactionServices.put(transactionSrv.hashCode(), transactionSrv);
        return transactionSrv;
    }

    public UUID UUID() {
        return session.uuid();
    }

    public boolean isOpen() {
        return isOpen.get();
    }

    public Grakn.Session session() {
        return session;
    }

    public Options.Session options() {
        return options;
    }

    public ByteString UUIDAsByteString() {
        return copyFrom(uuidToBytes(session.uuid()));
    }

    private void setIdleTimeout() {
        if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
        this.idleTimeoutTask = scheduled().schedule(this::triggerIdleTimeout, idleTimeoutMillis, MILLISECONDS);
    }

    private void triggerIdleTimeout() {
        if (!transactionServices.isEmpty()) {
            keepAlive();
            return;
        }
        close();
        LOG.warn("Session with ID " + session.uuid() + " timed out due to inactivity");
    }

    public synchronized void keepAlive() {
        setIdleTimeout();
    }

    public void remove(TransactionService transactionSrv) {
        transactionServices.remove(transactionSrv.hashCode());
    }

    public void close() {
        if (idleTimeoutTask != null) idleTimeoutTask.cancel(false);
        if (isOpen.compareAndSet(true, false)) {
            ConcurrentHashMap<Integer, TransactionService> copy = new ConcurrentHashMap<>(this.transactionServices);
            copy.values().parallelStream().forEach(TransactionService::close);
            session.close();
            graknSrv.remove(this);
        }
    }

    public void close(Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            ConcurrentHashMap<Integer, TransactionService> copy = new ConcurrentHashMap<>(this.transactionServices);
            copy.values().parallelStream().forEach(tr -> tr.close(error));
            session.close();
            graknSrv.remove(this);
        }
    }
}

/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.server.rpc;

import grabl.tracing.client.GrablTracingThreadStatic;
import grabl.tracing.client.GrablTracingThreadStatic.ThreadTrace;
import grakn.core.Grakn;
import grakn.core.GraknOptions;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.TransactionProto.Transaction;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static grabl.tracing.client.GrablTracingThreadStatic.continueTraceOnThread;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.Error.Session.SESSION_NOT_FOUND;
import static grakn.core.common.exception.Error.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.common.exception.Error.Transaction.UNEXPECTED_NULL;

/**
 * A StreamObserver that implements the transaction connection between a client
 * and the server. This class receives a stream of {@code Transaction.Req} and
 * returns a stream of {@code Transaction.Res}.
 */
class TransactionRPC implements StreamObserver<Transaction.Req> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionRPC.class);
    private final StreamObserver<Transaction.Res> responseSender;
    private final Iterators iterators;
    private final AtomicBoolean isOpen;
    private final Function<UUID, SessionRPC> sessionRPCSupplier;
    private SessionRPC sessionRPC;

    @Nullable
    private Grakn.Transaction transaction = null;

    TransactionRPC(Function<UUID, SessionRPC> sessionRPCSupplier, StreamObserver<Transaction.Res> responseSender) {
        this.sessionRPCSupplier = sessionRPCSupplier;
        this.responseSender = responseSender;
        this.iterators = new Iterators(responseSender::onNext);
        isOpen = new AtomicBoolean(false);
    }

    @Override
    public void onNext(Transaction.Req request) {
        try {
            if (GrablTracingThreadStatic.isTracingEnabled()) {
                Map<String, String> metadata = request.getMetadataMap();
                String rootId = metadata.get("traceRootId");
                String parentId = metadata.get("traceParentId");
                if (rootId != null && parentId != null) {
                    handleRequest(request, rootId, parentId);
                    return;
                }
            }
            handleRequest(request);
        } catch (Exception e) {
            close(e);
        }
    }

    @Override
    public void onCompleted() {
        close(null);
    }

    /**
     * This method is invoked when a client abruptly terminates a connection to
     * the server. So, we want to make sure to also close and delete the current
     * session and all the transactions associated to the same client.
     *
     * This might create issues if a session is used by other concurrent clients,
     * the better approach would be to signal a closure intent to the session
     * and the session should be able to detect whether it's been used by other
     * connections, if not close it completely.
     *
     * TODO: improve by sending close signal to the session
     *
     * @param error
     */
    @Override
    public void onError(Throwable error) {
        if (sessionRPC != null) sessionRPC.onError(error);
    }

    private <T> T nonNull(@Nullable T item) {
        if (item == null) {
            throw Status.INVALID_ARGUMENT.withDescription(UNEXPECTED_NULL.message()).asRuntimeException();
        } else {
            return item;
        }
    }

    private void handleRequest(Transaction.Req request, String rootId, String parentId) {
        try (ThreadTrace ignored = continueTraceOnThread(UUID.fromString(rootId), UUID.fromString(parentId), "handle")) {
            handleRequest(request);
        }
    }

    private void handleRequest(Transaction.Req request) {

    }

    private void handleIterRequest(Transaction.Iter.Req request) {

    }

    private void open(Transaction.Open.Req request) {
        UUID sessionID = bytesToUUID(request.getSessionID().toByteArray());
        sessionRPC = sessionRPCSupplier.apply(sessionID);

        if (sessionRPC == null) {
            throw Status.NOT_FOUND.withDescription(SESSION_NOT_FOUND.message(sessionID)).asRuntimeException();
        } else if (isOpen.compareAndSet(false, true)) {
            Grakn.Transaction.Type type = Grakn.Transaction.Type.of(request.getType().getNumber());
            if (type == null) throw Status.INVALID_ARGUMENT.asRuntimeException();

            transaction = sessionRPC.transaction(this, type);
            responseSender.onNext(ResponseBuilder.Transaction.open());
        } else {
            throw Status.ALREADY_EXISTS.withDescription(TRANSACTION_ALREADY_OPENED.message()).asRuntimeException();
        }
    }

    void close(@Nullable Throwable error) {
        if (isOpen.compareAndSet(true, false)) {
            if (transaction != null) {
                transaction.close();
                sessionRPC.remove(this);
            }

            if (error != null) {
                LOG.error(error.getMessage(), error);
                responseSender.onError(ResponseBuilder.exception(error));
            } else {
                responseSender.onCompleted();
            }
        }
    }

    /**
     * Contains a mutable map of iterators of TransactionProto.Transaction.Res for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
     * The iterators operate by batching results to reduce total round-trips.
     */
    static class Iterators {
        private final Consumer<Transaction.Res> responseSender;
        private final AtomicInteger iteratorIdCounter = new AtomicInteger(0);
        private final Map<Integer, BatchingIterator> iterators = new ConcurrentHashMap<>();

        Iterators(Consumer<Transaction.Res> responseSender) {
            this.responseSender = responseSender;
        }

        private static int getSizeFrom(Transaction.Iter.Req.Options options) {
            switch (options.getBatchSizeCase()) {
                case ALL:
                    return Integer.MAX_VALUE;
                case NUMBER:
                    return options.getNumber();
                case BATCHSIZE_NOT_SET:
                default:
                    return GraknOptions.DEFAULT_BATCH_SIZE;
            }
        }

        /**
         * Hand off an iterator and begin batch iterating.
         */
        void startBatchIterating(Iterator<Transaction.Res> iterator, Transaction.Iter.Req.Options options) {
            new BatchingIterator(iterator).iterateBatch(options);
        }

        /**
         * Iterate the next batch of an existing iterator.
         */
        void resumeBatchIterating(int iteratorId, Transaction.Iter.Req.Options options) {
            BatchingIterator iterator = iterators.get(iteratorId);
            if (iterator == null) {
                throw Status.FAILED_PRECONDITION.asRuntimeException();
            }
            iterator.iterateBatch(options);
        }

        void stop(int iteratorId) {
            iterators.remove(iteratorId);
        }

        private int saveIterator(BatchingIterator iterator) {
            int id = iteratorIdCounter.incrementAndGet();
            iterators.put(id, iterator);
            return id;
        }

        class BatchingIterator {
            private final Iterator<Transaction.Res> iterator;
            private int id = -1;

            BatchingIterator(Iterator<Transaction.Res> iterator) {
                this.iterator = iterator;
            }

            private boolean isSaved() {
                return id != -1;
            }

            void iterateBatch(Transaction.Iter.Req.Options options) {
                int batchSize = getSizeFrom(options);
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                    responseSender.accept(iterator.next());
                }

                if (iterator.hasNext()) {
                    if (!isSaved()) id = saveIterator(this);
                    responseSender.accept(ResponseBuilder.Transaction.Iter.id(id));
                } else {
                    if (isSaved()) stop(id);
                    responseSender.accept(ResponseBuilder.Transaction.Iter.done());
                }
            }
        }
    }
}

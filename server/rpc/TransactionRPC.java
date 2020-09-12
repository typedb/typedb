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
import grakn.core.common.exception.ErrorMessage;
import grakn.core.common.exception.GraknException;
import grakn.core.common.parameters.Arguments;
import grakn.core.common.parameters.Options;
import grakn.core.concept.thing.Thing;
import grakn.core.concept.type.AttributeType;
import grakn.core.concept.type.EntityType;
import grakn.core.concept.type.RelationType;
import grakn.core.concept.type.Type;
import grakn.core.server.rpc.ConceptRPC.ConceptHolder;
import grakn.core.server.rpc.util.RequestReader;
import grakn.core.server.rpc.util.ResponseBuilder;
import grakn.protocol.ConceptProto;
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
import static grabl.tracing.client.GrablTracingThreadStatic.traceOnThread;
import static grakn.core.common.collection.Bytes.bytesToUUID;
import static grakn.core.common.exception.ErrorMessage.Session.SESSION_NOT_FOUND;
import static grakn.core.common.exception.ErrorMessage.Transaction.TRANSACTION_ALREADY_OPENED;
import static grakn.core.common.exception.ErrorMessage.Transaction.UNEXPECTED_NULL;
import static java.lang.String.format;

/**
 * A StreamObserver that implements the transaction connection between a client
 * and the server. This class receives a stream of {@code Transaction.Req} and
 * returns a stream of {@code Transaction.Res}.
 */
public class TransactionRPC implements StreamObserver<Transaction.Req> {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionRPC.class);
    private final StreamObserver<Transaction.Res> responseSender;
    private final AtomicBoolean isOpen;
    private final Function<UUID, SessionRPC> sessionRPCSupplier;

    private SessionRPC sessionRPC;
    private Grakn.Transaction transaction;
    private Arguments.Transaction.Type transactionType;
    private Options.Transaction transactionOptions;
    private Iterators iterators;

    TransactionRPC(Function<UUID, SessionRPC> sessionRPCSupplier, StreamObserver<Transaction.Res> responseSender) {
        this.sessionRPCSupplier = sessionRPCSupplier;
        this.responseSender = responseSender;
        isOpen = new AtomicBoolean(false);
    }

    public Arguments.Transaction.Type type() {
        return transactionType;
    }

    public Options.Transaction options() {
        return transactionOptions;
    }

    @Override
    public void onNext(Transaction.Req request) {
        try {
            LOG.trace("Request: {}", request);

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
        switch (request.getReqCase()) {
            case OPEN_REQ:
                open(request.getOpenReq());
                break;
            // TODO: add cases
            case COMMIT_REQ:
                commit();
                break;
            case ITER_REQ:
                handleIterRequest(request.getIterReq());
                break;
            case GETTYPE_REQ:
                getType(request.getGetTypeReq());
                break;
            case GETTHING_REQ:
                getThing(request.getGetThingReq());
                break;
            case PUTENTITYTYPE_REQ:
                putEntityType(request.getPutEntityTypeReq());
                break;
            case PUTATTRIBUTETYPE_REQ:
                putAttributeType(request.getPutAttributeTypeReq());
                break;
            case PUTRELATIONTYPE_REQ:
                putRelationType(request.getPutRelationTypeReq());
                break;
//            case PUTRULE_REQ:
//                putRule(request.getPutRuleReq());
//                break;
            case CONCEPTMETHOD_THING_REQ:
                thingReq(request.getConceptMethodThingReq());
                break;
            case CONCEPTMETHOD_TYPE_REQ:
                typeReq(request.getConceptMethodTypeReq());
                break;
//            case EXPLANATION_REQ:
//                explanation(request.getExplanationReq());
//                break;
            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    private void handleIterRequest(Transaction.Iter.Req request) {
        switch (request.getReqCase()) {
            case ITERATORID:
                iterators.resumeBatchIterating(request.getIteratorID());
                break;
            case QUERY_ITER_REQ:
                query(request.getQueryIterReq());
                break;
            case CONCEPTMETHOD_THING_ITER_REQ:
                thingIterReq(request.getConceptMethodThingIterReq());
                break;
            case CONCEPTMETHOD_TYPE_ITER_REQ:
                typeIterReq(request.getConceptMethodTypeIterReq());
                break;
            default:
            case REQ_NOT_SET:
                throw new GraknException(ErrorMessage.Server.UNKNOWN_REQUEST_TYPE);
        }
    }

    private void open(Transaction.Open.Req request) {
        UUID sessionID = bytesToUUID(request.getSessionID().toByteArray());
        sessionRPC = sessionRPCSupplier.apply(sessionID);

        if (sessionRPC == null) {
            throw Status.NOT_FOUND.withDescription(SESSION_NOT_FOUND.message(sessionID)).asRuntimeException();
        } else if (isOpen.compareAndSet(false, true)) {
            transactionType = Arguments.Transaction.Type.of(request.getType().getNumber());
            transactionOptions = RequestReader.getOptions(Options.Transaction::new, request.getOptions());
            if (transactionType == null) throw Status.INVALID_ARGUMENT.asRuntimeException();

            transaction = sessionRPC.transaction(this);
            iterators = new Iterators(responseSender::onNext, transaction.options().batchSize());
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

    private Grakn.Transaction transaction() {
        return nonNull(transaction);
    }

    private void query(Transaction.Query.Iter.Req request) {
        try (ThreadTrace ignored = traceOnThread("query")) {
            Options.Query options = RequestReader.getOptions(Options.Query::new, request.getOptions());

            // TODO
            // Stream<Transaction.Res> responseStream = transaction().query().insert(request.getQuery(), options)
            //        .map(ResponseBuilder.Transaction.Iter::query);
            // iterators.startBatchIterating(responseStream.iterator(), queryOptions);
        }
    }

    private void commit() {
        transaction().commit();
        responseSender.onNext(ResponseBuilder.Transaction.commit());
    }

    private void getType(Transaction.GetType.Req request) {
        final Type type = transaction().concepts().getType(request.getLabel());
        final Transaction.Res response = ResponseBuilder.Transaction.getType(type);
        responseSender.onNext(response);
    }

    private void getThing(Transaction.GetThing.Req request) {
        final Thing thing = transaction().concepts().getThing(request.getIid().toByteArray());
        final Transaction.Res response = ResponseBuilder.Transaction.getThing(thing);
        responseSender.onNext(response);
    }

    private void putEntityType(Transaction.PutEntityType.Req request) {
        final EntityType entityType = transaction().concepts().putEntityType(request.getLabel());
        final Transaction.Res response = ResponseBuilder.Transaction.putEntityType(entityType);
        responseSender.onNext(response);
    }

    private void putAttributeType(Transaction.PutAttributeType.Req request) {
        final ConceptProto.AttributeType.VALUE_TYPE valueTypeProto = request.getValueType();
        final AttributeType.ValueType valueType;
        switch (valueTypeProto) {
            case STRING:
                valueType = AttributeType.ValueType.STRING;
                break;
            case BOOLEAN:
                valueType = AttributeType.ValueType.BOOLEAN;
                break;
            case LONG:
                valueType = AttributeType.ValueType.LONG;
                break;
            case DOUBLE:
                valueType = AttributeType.ValueType.DOUBLE;
                break;
            case DATETIME:
                valueType = AttributeType.ValueType.DATETIME;
                break;
            default:
            case OBJECT:
            case UNRECOGNIZED:
                throw Status.UNIMPLEMENTED.withDescription(format("Unsupported value type '%s'", valueTypeProto)).asRuntimeException();
        }
        final AttributeType attributeType = transaction.concepts().putAttributeType(request.getLabel(), valueType);
        final Transaction.Res response = ResponseBuilder.Transaction.putAttributeType(attributeType);
        responseSender.onNext(response);
    }

    private void putRelationType(Transaction.PutRelationType.Req request) {
        RelationType relationType = transaction().concepts().putRelationType(request.getLabel());
        Transaction.Res response = ResponseBuilder.Transaction.putRelationType(relationType);
        responseSender.onNext(response);
    }

//    private void putRule(Transaction.PutRule.Req request) {
//        Label label = Label.of(request.getLabel());
//        Pattern when = Graql.parsePattern(request.getWhen());
//        Pattern then = Graql.parsePattern(request.getThen());
//
//        Rule rule = transaction().putRule(label, when, then);
//        Transaction.Res response = ResponseBuilder.Transaction.putRule(rule);
//        responseSender.onNext(response);
//    }

    private void thingReq(Transaction.ConceptMethod.Thing.Req thingReq) {
        final ConceptHolder con = new ConceptHolder(transaction(), thingReq.getIid(), iterators, responseSender::onNext);
        ConceptRPC.run(con, thingReq.getMethod());
    }

    private void thingIterReq(Transaction.ConceptMethod.Thing.Iter.Req thingReq) {
        final ConceptHolder con = new ConceptHolder(transaction(), thingReq.getIid(), iterators, responseSender::onNext);
        ConceptRPC.iter(con, thingReq.getMethod());
    }

    private void typeReq(Transaction.ConceptMethod.Type.Req typeReq) {
        final ConceptHolder con = new ConceptHolder(transaction(), typeReq.getLabel(), typeReq.getScope(), iterators, responseSender::onNext);
        ConceptRPC.run(con, typeReq.getMethod());
    }

    private void typeIterReq(Transaction.ConceptMethod.Type.Iter.Req typeReq) {
        final ConceptHolder con = new ConceptHolder(transaction(), typeReq.getLabel(), typeReq.getScope(), iterators, responseSender::onNext);
        ConceptRPC.iter(con, typeReq.getMethod());
    }

//    /**
//     * Reconstruct local ConceptMap and return the explanation associated with the ConceptMap provided by the user
//     */
//    private void explanation(AnswerProto.Explanation.Req explanationReq) {
//        responseSender.onError(Status.UNIMPLEMENTED.asException());
//        // TODO: implement TransactionListener.explanation()
//    }

    /**
     * Contains a mutable map of iterators of TransactionProto.Transaction.Res for gRPC. These iterators are used for returning
     * lazy, streaming responses such as for Graql query results.
     *
     * The iterators operate by batching results to reduce total round-trips.
     */
    public static class Iterators {
        private final int batchSize;
        private final Consumer<Transaction.Res> responseSender;
        private final AtomicInteger iteratorIdCounter = new AtomicInteger(0);
        private final Map<Integer, BatchingIterator> iterators = new ConcurrentHashMap<>();

        Iterators(Consumer<Transaction.Res> responseSender, final int batchSize) {
            this.responseSender = responseSender;
            this.batchSize = batchSize;
        }

        /**
         * Hand off an iterator and begin batch iterating.
         */
        void startBatchIterating(Iterator<Transaction.Res> iterator) {
            new BatchingIterator(iterator).iterateBatch();
        }

        /**
         * Iterate the next batch of an existing iterator.
         */
        void resumeBatchIterating(int iteratorId) {
            BatchingIterator iterator = iterators.get(iteratorId);
            if (iterator == null) {
                throw Status.FAILED_PRECONDITION.asRuntimeException();
            }
            iterator.iterateBatch();
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

            void iterateBatch() {
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

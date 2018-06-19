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
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Grakn. If not, see <http://www.gnu.org/licenses/agpl.txt>.
 */

package ai.grakn.engine.rpc;

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.engine.util.EmbeddedConceptReader;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Streamable;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.RPCIterators;
import ai.grakn.rpc.RPCOpener;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.AttributeValue;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.ExecQuery;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.PutAttributeType;
import ai.grakn.rpc.generated.GrpcGrakn.PutRule;
import ai.grakn.rpc.generated.GrpcGrakn.RunConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.generated.GrpcIterator.Next;
import ai.grakn.rpc.generated.GrpcIterator.Stop;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ConceptReader;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.TxConceptReader;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static ai.grakn.engine.rpc.Service.nonNull;

/**
 * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link Server}.
 * Receives a stream of {@link TxRequest}s and returning a stream of {@link TxResponse}s.
 *
 * @author Felix Chapman
 */
class Listener implements StreamObserver<TxRequest> {
    final Logger LOG = LoggerFactory.getLogger(Listener.class);
    private final StreamObserver<TxResponse> reponseSender;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final ExecutorService threadExecutor;
    private final RPCOpener requestExecutor;
    private final PostProcessor postProcessor;
    private final RPCIterators rpcIterators = RPCIterators.create();

    @Nullable
    private EmbeddedGraknTx<?> tx = null;

    private Listener(StreamObserver<TxResponse> responseSender, ExecutorService threadExecutor, RPCOpener requestExecutor, PostProcessor postProcessor) {
        this.reponseSender = responseSender;
        this.threadExecutor = threadExecutor;
        this.requestExecutor = requestExecutor;
        this.postProcessor = postProcessor;
    }

    public static Listener create(StreamObserver<TxResponse> responseObserver, RPCOpener requestExecutor, PostProcessor postProcessor) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
        ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
        return new Listener(responseObserver, threadExecutor, requestExecutor, postProcessor);
    }

    @Override
    public void onNext(TxRequest request) {
        try {
            submit(() -> {
                Service.runAndConvertGraknExceptions(() -> handleRequest(request));
            });
        } catch (StatusRuntimeException e) {
            close(e);
        }
    }

    private void handleRequest(TxRequest request) {
        switch (request.getRequestCase()) {
            case OPEN:
                open(request.getOpen());
                break;
            case COMMIT:
                commit();
                break;
            case EXECQUERY:
                execQuery(request.getExecQuery());
                break;
            case NEXT:
                next(request.getNext());
                break;
            case STOP:
                stop(request.getStop());
                break;
            case RUNCONCEPTMETHOD:
                runConceptMethod(request.getRunConceptMethod());
                break;
            case GETCONCEPT:
                getConcept(request.getGetConcept());
                break;
            case GETSCHEMACONCEPT:
                getSchemaConcept(request.getGetSchemaConcept());
                break;
            case GETATTRIBUTESBYVALUE:
                getAttributesByValue(request.getGetAttributesByValue());
                break;
            case PUTENTITYTYPE:
                putEntityType(request.getPutEntityType());
                break;
            case PUTRELATIONSHIPTYPE:
                putRelationshipType(request.getPutRelationshipType());
                break;
            case PUTATTRIBUTETYPE:
                putAttributeType(request.getPutAttributeType());
                break;
            case PUTROLE:
                putRole(request.getPutRole());
                break;
            case PUTRULE:
                putRule(request.getPutRule());
                break;
            default:
            case REQUEST_NOT_SET:
                throw Service.error(Status.INVALID_ARGUMENT);
        }
    }

    @Override
    public void onError(Throwable t) {
        close(t);
    }

    @Override
    public void onCompleted() {
        close(null);
    }

    public void close(@Nullable Throwable error) {
        submit(() -> {
            if (tx != null) {
                tx.close();
            }
        });

        if (!terminated.getAndSet(true)) {
            if (error != null) {
                LOG.error("Runtime Exception in RPC Listener: ", error);
                reponseSender.onError(error);
            } else {
                reponseSender.onCompleted();
            }
        }

        threadExecutor.shutdown();
    }

    private void submit(Runnable runnable) {
        try {
            threadExecutor.submit(runnable).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assert cause instanceof RuntimeException : "No checked exceptions are thrown, because it's a `Runnable`";
            throw (RuntimeException) cause;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void open(Open request) {
        if (tx != null) {
            throw Service.error(Status.FAILED_PRECONDITION);
        }
        tx = requestExecutor.execute(request);
        reponseSender.onNext(ResponseBuilder.done());
    }

    private void commit() {
        tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
        reponseSender.onNext(ResponseBuilder.done());
    }

    private void execQuery(ExecQuery request) {
        String queryString = request.getQuery().getValue();
        QueryBuilder graql = tx().graql();
        TxResponse response;

        if (request.hasInfer()) {
            graql = graql.infer(request.getInfer().getValue());
        }

        Query<?> query = graql.parse(queryString);

        if (query instanceof Streamable) {
            Stream<GrpcGrakn.TxResponse> responseStream = ((Streamable<?>) query).stream().map(ResponseBuilder::answer);
            IteratorId iteratorId = rpcIterators.add(responseStream.iterator());

            response = TxResponse.newBuilder().setIteratorId(iteratorId).build();
        } else {
            Object result = query.execute();

            if (result == null) response = ResponseBuilder.done();
            else response = ResponseBuilder.answer(result);
        }

        reponseSender.onNext(response);
    }

    private void next(Next next) {
        IteratorId iteratorId = next.getIteratorId();

        TxResponse response =
                rpcIterators.next(iteratorId).orElseThrow(() -> Service.error(Status.FAILED_PRECONDITION));

        reponseSender.onNext(response);
    }

    private void stop(Stop stop) {
        IteratorId iteratorId = stop.getIteratorId();
        rpcIterators.stop(iteratorId);
        reponseSender.onNext(ResponseBuilder.done());
    }

    private void runConceptMethod(RunConceptMethod runConceptMethod) {
        Concept concept = nonNull(tx().getConcept(ConceptId.of(runConceptMethod.getId().getValue())));
        TxConceptReader txConceptReader = new EmbeddedConceptReader(tx());

        TxResponse response = ConceptMethod.run(concept, runConceptMethod.getConceptMethod(), rpcIterators, txConceptReader);
        reponseSender.onNext(response);
    }

    private void getConcept(GrpcConcept.ConceptId conceptId) {
        TxResponse.Builder response = TxResponse.newBuilder();
        Concept concept = tx().getConcept(ConceptId.of(conceptId.getValue()));

        if (concept != null) {
            response.setConcept(ConceptBuilder.concept(concept));
        } else {
            response.setNoResult(true);
        }

        reponseSender.onNext(response.build());
    }

    private void getSchemaConcept(String label) {
        TxResponse.Builder response = TxResponse.newBuilder();
        Concept concept = tx().getSchemaConcept(Label.of(label));

        if (concept != null) {
            response.setConcept(ConceptBuilder.concept(concept));
        } else {
            response.setNoResult(true);
        }

        reponseSender.onNext(response.build());
    }

    private void getAttributesByValue(AttributeValue attributeValue) {
        Object value = attributeValue.getAllFields().values().iterator().next();
        Collection<Attribute<Object>> attributes = tx().getAttributesByValue(value);

        Iterator<TxResponse> iterator = attributes.stream().map(ResponseBuilder::concept).iterator();
        IteratorId iteratorId = rpcIterators.add(iterator);

        reponseSender.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
    }

    private void putEntityType(String label) {
        EntityType entityType = tx().putEntityType(Label.of(label));
        reponseSender.onNext(ResponseBuilder.concept(entityType));
    }

    private void putRelationshipType(String label) {
        RelationshipType relationshipType = tx().putRelationshipType(Label.of(label));
        reponseSender.onNext(ResponseBuilder.concept(relationshipType));
    }

    private void putAttributeType(PutAttributeType putAttributeType) {
        Label label = Label.of(putAttributeType.getLabel());
        AttributeType.DataType<?> dataType = ConceptReader.dataType(putAttributeType.getDataType());

        AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
        reponseSender.onNext(ResponseBuilder.concept(attributeType));
    }

    private void putRole(String label) {
        Role role = tx().putRole(Label.of(label));
        reponseSender.onNext(ResponseBuilder.concept(role));
    }

    private void putRule(PutRule putRule) {
        Label label = Label.of(putRule.getLabel());
        Pattern when = Graql.parser().parsePattern(putRule.getWhen());
        Pattern then = Graql.parser().parsePattern(putRule.getThen());

        Rule rule = tx().putRule(label, when, then);
        reponseSender.onNext(ResponseBuilder.concept(rule));
    }

    private EmbeddedGraknTx<?> tx() {
        return nonNull(tx);
    }
}

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

package ai.grakn.engine.rpc;

/*-
 * #%L
 * grakn-engine
 * %%
 * Copyright (C) 2016 - 2018 Grakn Labs Ltd
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.engine.task.postprocessing.PostProcessor;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Streamable;
import ai.grakn.grpc.ConceptMethod;
import ai.grakn.grpc.ConceptMethods;
import ai.grakn.grpc.GrpcConceptConverter;
import ai.grakn.grpc.GrpcIterators;
import ai.grakn.grpc.GrpcOpenRequestExecutor;
import ai.grakn.grpc.GrpcUtil;
import ai.grakn.kb.internal.EmbeddedGraknTx;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcConcept.AttributeValue;
import ai.grakn.rpc.generated.GrpcGrakn.ExecQuery;
import ai.grakn.rpc.generated.GrpcGrakn.Open;
import ai.grakn.rpc.generated.GrpcGrakn.PutAttributeType;
import ai.grakn.rpc.generated.GrpcGrakn.PutRule;
import ai.grakn.rpc.generated.GrpcGrakn.QueryResult;
import ai.grakn.rpc.generated.GrpcGrakn.RunConceptMethod;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.generated.GrpcIterator.Next;
import ai.grakn.rpc.generated.GrpcIterator.Stop;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static ai.grakn.engine.rpc.GrpcGraknService.nonNull;

/**
 * A {@link StreamObserver} that implements the transaction-handling behaviour for {@link GrpcServer}.
 * <p>
 * <p>
 * Receives a stream of {@link TxRequest}s and returning a stream of {@link TxResponse}s.
 * </p>
 *
 * @author Felix Chapman
 */
class TxObserver implements StreamObserver<TxRequest> {

    private final StreamObserver<TxResponse> responseObserver;
    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private final ExecutorService threadExecutor;
    private final GrpcOpenRequestExecutor requestExecutor;
    private final PostProcessor postProcessor;
    private final GrpcIterators grpcIterators = GrpcIterators.create();

    @Nullable
    private EmbeddedGraknTx<?> tx = null;

    private TxObserver(StreamObserver<TxResponse> responseObserver, ExecutorService threadExecutor, GrpcOpenRequestExecutor requestExecutor, PostProcessor postProcessor) {
        this.responseObserver = responseObserver;
        this.threadExecutor = threadExecutor;
        this.requestExecutor = requestExecutor;
        this.postProcessor = postProcessor;
    }

    public static TxObserver create(StreamObserver<TxResponse> responseObserver, GrpcOpenRequestExecutor requestExecutor, PostProcessor postProcessor) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("tx-observer-%s").build();
        ExecutorService threadExecutor = Executors.newSingleThreadExecutor(threadFactory);
        return new TxObserver(responseObserver, threadExecutor, requestExecutor, postProcessor);
    }

    @Override
    public void onNext(TxRequest request) {
        try {
            submit(() -> {
                GrpcGraknService.runAndConvertGraknExceptions(() -> handleRequest(request));
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
                throw GrpcGraknService.error(Status.INVALID_ARGUMENT);
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
                responseObserver.onError(error);
            } else {
                responseObserver.onCompleted();
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
            throw GrpcGraknService.error(Status.FAILED_PRECONDITION);
        }
        tx = requestExecutor.execute(request);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void commit() {
        tx().commitSubmitNoLogs().ifPresent(postProcessor::submit);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void execQuery(ExecQuery request) {
        String queryString = request.getQuery().getValue();

        QueryBuilder graql = tx().graql();

        if (request.hasInfer()) {
            graql = graql.infer(request.getInfer().getValue());
        }

        Query<?> query = graql.parse(queryString);

        GrpcConverter grpcConverter = GrpcConverter.get();

        if (query instanceof Streamable) {
            Stream<QueryResult> queryResultStream = ((Streamable<?>) query).stream().map(grpcConverter::convert);

            Stream<TxResponse> txResponseStream =
                    queryResultStream.map(this::txResponse);

            Iterator<TxResponse> iterator = txResponseStream.iterator();

            IteratorId iteratorId = grpcIterators.add(iterator);

            responseObserver.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
        } else {
            Object result = query.execute();

            if (result == null) {
                responseObserver.onNext(GrpcUtil.doneResponse());
            } else {
                responseObserver.onNext(txResponse(grpcConverter.convert(result)));
            }
        }
    }

    private TxResponse txResponse(QueryResult queryResult) {
        return TxResponse.newBuilder().setQueryResult(queryResult).build();
    }

    private void next(Next next) {
        IteratorId iteratorId = next.getIteratorId();

        TxResponse response =
                grpcIterators.next(iteratorId).orElseThrow(() -> GrpcGraknService.error(Status.FAILED_PRECONDITION));

        responseObserver.onNext(response);
    }

    private void stop(Stop stop) {
        IteratorId iteratorId = stop.getIteratorId();
        grpcIterators.stop(iteratorId);
        responseObserver.onNext(GrpcUtil.doneResponse());
    }

    private void runConceptMethod(RunConceptMethod runConceptMethod) {
        Concept concept = nonNull(tx().getConcept(GrpcUtil.getConceptId(runConceptMethod)));

        GrpcConceptConverter converter = grpcConcept -> tx().getConcept(GrpcUtil.convert(grpcConcept.getId()));

        ConceptMethod<?> conceptMethod = ConceptMethods.fromGrpc(converter, runConceptMethod.getConceptMethod());

        TxResponse response = conceptMethod.run(grpcIterators, concept);

        responseObserver.onNext(response);
    }

    private void getConcept(GrpcConcept.ConceptId conceptId) {
        Optional<Concept> concept = Optional.ofNullable(tx().getConcept(GrpcUtil.convert(conceptId)));

        TxResponse response =
                TxResponse.newBuilder().setOptionalConcept(GrpcUtil.convertOptionalConcept(concept)).build();

        responseObserver.onNext(response);
    }

    private void getSchemaConcept(GrpcConcept.Label label) {
        Optional<Concept> concept = Optional.ofNullable(tx().getSchemaConcept(GrpcUtil.convert(label)));

        TxResponse response =
                TxResponse.newBuilder().setOptionalConcept(GrpcUtil.convertOptionalConcept(concept)).build();

        responseObserver.onNext(response);
    }

    private void getAttributesByValue(AttributeValue attributeValue) {
        Collection<Attribute<Object>> attributes = tx().getAttributesByValue(GrpcUtil.convert(attributeValue));

        Iterator<TxResponse> iterator = attributes.stream().map(GrpcUtil::conceptResponse).iterator();
        IteratorId iteratorId = grpcIterators.add(iterator);

        responseObserver.onNext(TxResponse.newBuilder().setIteratorId(iteratorId).build());
    }

    private void putEntityType(GrpcConcept.Label label) {
        EntityType entityType = tx().putEntityType(GrpcUtil.convert(label));
        responseObserver.onNext(GrpcUtil.conceptResponse(entityType));
    }

    private void putRelationshipType(GrpcConcept.Label label) {
        RelationshipType relationshipType = tx().putRelationshipType(GrpcUtil.convert(label));
        responseObserver.onNext(GrpcUtil.conceptResponse(relationshipType));
    }

    private void putAttributeType(PutAttributeType putAttributeType) {
        Label label = GrpcUtil.convert(putAttributeType.getLabel());
        AttributeType.DataType<?> dataType = GrpcUtil.convert(putAttributeType.getDataType());

        AttributeType<?> attributeType = tx().putAttributeType(label, dataType);
        responseObserver.onNext(GrpcUtil.conceptResponse(attributeType));
    }

    private void putRole(GrpcConcept.Label label) {
        Role role = tx().putRole(GrpcUtil.convert(label));
        responseObserver.onNext(GrpcUtil.conceptResponse(role));
    }

    private void putRule(PutRule putRule) {
        Label label = GrpcUtil.convert(putRule.getLabel());
        Pattern when = GrpcUtil.convert(putRule.getWhen());
        Pattern then = GrpcUtil.convert(putRule.getThen());

        Rule rule = tx().putRule(label, when, then);
        responseObserver.onNext(GrpcUtil.conceptResponse(rule));
    }

    private EmbeddedGraknTx<?> tx() {
        return nonNull(tx);
    }
}

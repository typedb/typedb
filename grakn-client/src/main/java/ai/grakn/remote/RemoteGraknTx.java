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

package ai.grakn.remote;

import ai.grakn.GraknSession;
import ai.grakn.GraknTx;
import ai.grakn.GraknTxType;
import ai.grakn.QueryExecutor;
import ai.grakn.concept.Attribute;
import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.EntityType;
import ai.grakn.concept.Label;
import ai.grakn.concept.RelationshipType;
import ai.grakn.concept.Role;
import ai.grakn.concept.Rule;
import ai.grakn.concept.SchemaConcept;
import ai.grakn.concept.Type;
import ai.grakn.exception.InvalidKBException;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.ComputeQueryImpl;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.graql.internal.query.QueryBuilderImpl;
import ai.grakn.kb.admin.GraknAdmin;
import ai.grakn.remote.rpc.RemoteConceptReader;
import ai.grakn.remote.rpc.RemoteIterator;
import ai.grakn.remote.rpc.RequestBuilder;
import ai.grakn.rpc.RPCCommunicator;
import ai.grakn.rpc.generated.GraknGrpc.GraknStub;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.DeleteRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcIterator;
import ai.grakn.rpc.util.ConceptBuilder;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMap;
import io.grpc.StatusRuntimeException;
import mjson.Json;

import javax.annotation.Nullable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static ai.grakn.util.CommonUtil.toImmutableSet;

/**
 * Remote implementation of {@link GraknTx} and {@link GraknAdmin} that communicates with a Grakn server using gRPC.
 */
public final class RemoteGraknTx implements GraknTx, GraknAdmin {

    private final RemoteGraknSession session;
    private final GraknTxType txType;
    private final RPCCommunicator communicator;
    private final TxConceptReader conceptReader;

    private RemoteGraknTx(RemoteGraknSession session, GraknTxType txType, TxRequest openRequest, GraknStub stub) {
        this.session = session;
        this.txType = txType;
        this.communicator = RPCCommunicator.create(stub);
        this.conceptReader = new RemoteConceptReader(this);
        communicator.send(openRequest);
        responseOrThrow();
    }

    // TODO: ideally the transaction should not hold a reference to the session or at least depend on a session interface
    public static RemoteGraknTx create(RemoteGraknSession session, TxRequest openRequest) {
        GraknStub stub = session.stub();
        return new RemoteGraknTx(session, GraknTxType.of(openRequest.getOpen().getTxType().getNumber()), openRequest, stub);
    }


    private GrpcGrakn.TxResponse responseOrThrow() {
        RPCCommunicator.Response response;

        try {
            response = communicator.receive();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // This is called from classes like RemoteGraknTx, that impl methods which do not throw InterruptedException
            // Therefore, we have to wrap it in a RuntimeException.
            throw new RuntimeException(e);
        }

        switch (response.type()) {
            case OK:
                return response.ok();
            case ERROR:
                throw convertStatusRuntimeException(response.error());
            case COMPLETED:
            default:
                throw CommonUtil.unreachableStatement("Unexpected response " + response);
        }
    }

    private static RuntimeException convertStatusRuntimeException(StatusRuntimeException error) {
        ResponseBuilder.ErrorType errorType = error.getTrailers().get(ResponseBuilder.ErrorType.KEY);

        if (errorType != null) return errorType.toException(error.getStatus().getDescription());
        else return error;
    }

    public TxConceptReader conceptReader() {
        return conceptReader;
    }

    public GrpcGrakn.TxResponse next(GrpcIterator.IteratorId iteratorId) {
        communicator.send(RequestBuilder.next(iteratorId));
        return responseOrThrow();
    }

    public GrpcGrakn.TxResponse runConceptMethod(ConceptId id, GrpcConcept.ConceptMethod method) {
        GrpcGrakn.RunConceptMethod.Builder runConceptMethod = GrpcGrakn.RunConceptMethod.newBuilder();
        runConceptMethod.setId(ConceptBuilder.conceptId(id));
        runConceptMethod.setConceptMethod(method);
        GrpcGrakn.TxRequest conceptMethodRequest = GrpcGrakn.TxRequest.newBuilder().setRunConceptMethod(runConceptMethod).build();

        communicator.send(conceptMethodRequest);
        return responseOrThrow();
    }

    @Override
    public EntityType putEntityType(Label label) {
        communicator.send(RequestBuilder.putEntityType(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asEntityType();
    }

    @Override
    public <V> AttributeType<V> putAttributeType(Label label, AttributeType.DataType<V> dataType) {
        communicator.send(RequestBuilder.putAttributeType(label, dataType));
        return conceptReader.concept(responseOrThrow().getConcept()).asAttributeType();
    }

    @Override
    public Rule putRule(Label label, Pattern when, Pattern then) {
        communicator.send(RequestBuilder.putRule(label, when, then));
        return conceptReader.concept(responseOrThrow().getConcept()).asRule();
    }

    @Override
    public RelationshipType putRelationshipType(Label label) {
        communicator.send(RequestBuilder.putRelationshipType(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asRelationshipType();
    }

    @Override
    public Role putRole(Label label) {
        communicator.send(RequestBuilder.putRole(label));
        return conceptReader.concept(responseOrThrow().getConcept()).asRole();
    }

    @Nullable
    @Override
    public <T extends Concept> T getConcept(ConceptId id) {
        communicator.send(RequestBuilder.getConcept(id));
        GrpcGrakn.TxResponse response = responseOrThrow();
        if (response.getNoResult()) return null;
        return (T) conceptReader.concept(response.getConcept());
    }

    @Nullable
    @Override
    public <T extends SchemaConcept> T getSchemaConcept(Label label) {
        communicator.send(RequestBuilder.getSchemaConcept(label));
        GrpcGrakn.TxResponse response = responseOrThrow();
        if (response.getNoResult()) return null;
        return (T) conceptReader.concept(response.getConcept());
    }

    @Nullable
    @Override
    public <T extends Type> T getType(Label label) {
        return getSchemaConcept(label);
    }

    @Override
    public <V> Collection<Attribute<V>> getAttributesByValue(V value) {
        communicator.send(RequestBuilder.getAttributesByValue(value));
        GrpcIterator.IteratorId iteratorId = responseOrThrow().getIteratorId();
        Iterable<Concept> iterable = () -> new RemoteIterator<>(
                this, iteratorId, response -> conceptReader.concept(response.getConcept())
        );

        return StreamSupport.stream(iterable.spliterator(), false).map(Concept::<V>asAttribute).collect(toImmutableSet());
    }

    @Nullable
    @Override
    public EntityType getEntityType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public RelationshipType getRelationshipType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public <V> AttributeType<V> getAttributeType(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public Role getRole(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Nullable
    @Override
    public Rule getRule(String label) {
        return getSchemaConcept(Label.of(label));
    }

    @Override
    public GraknAdmin admin() {
        return this;
    }

    @Override
    public GraknTxType txType() {
        return txType;
    }

    @Override
    public GraknSession session() {
        return session;
    }

    @Override
    public boolean isClosed() {
        return communicator.isClosed();
    }

    @Override
    public QueryBuilder graql() {
        return new QueryBuilderImpl(this);
    }

    @Override
    public void close() {
        communicator.close();
    }

    @Override
    public void commit() throws InvalidKBException {
        communicator.send(RequestBuilder.commit());
        responseOrThrow();
        close();
    }

    @Override
    public Stream<SchemaConcept> sups(SchemaConcept schemaConcept) {
        GrpcConcept.ConceptMethod.Builder method = GrpcConcept.ConceptMethod.newBuilder();
        method.setGetSuperConcepts(GrpcConcept.Unit.getDefaultInstance());
        GrpcIterator.IteratorId iteratorId = runConceptMethod(schemaConcept.getId(), method.build()).getConceptResponse().getIteratorId();
        Iterable<? extends Concept> iterable = () -> new RemoteIterator<>(
                this, iteratorId, res -> this.conceptReader().concept(res.getConcept())
        );

        Stream<? extends Concept> sups = StreamSupport.stream(iterable.spliterator(), false);
        return Objects.requireNonNull(sups).map(Concept::asSchemaConcept);
    }

    @Override
    public void delete() {
        DeleteRequest request = RequestBuilder.delete(RequestBuilder.open(keyspace(), GraknTxType.WRITE).getOpen());
        session.blockingStub().delete(request);
        close();
    }

    @Override
    public QueryExecutor queryExecutor() {
        return RemoteQueryExecutor.create(this);
    }

    public Iterator<Object> execQuery(Query<?> query) {
        communicator.send(RequestBuilder.execQuery(query.toString(), query.inferring()));

        GrpcGrakn.TxResponse txResponse = responseOrThrow();

        switch (txResponse.getResponseCase()) {
            case ANSWER:
                return Collections.singleton(answer(txResponse.getAnswer())).iterator();
            case DONE:
                return Collections.emptyIterator();
            case ITERATORID:
                GrpcIterator.IteratorId iteratorId = txResponse.getIteratorId();
                return new RemoteIterator<>(this, iteratorId, response -> answer(response.getAnswer()));
            default:
                throw CommonUtil.unreachableStatement("Unexpected " + txResponse);
        }
    }

    private Object answer(GrpcGrakn.Answer answer) {
        switch (answer.getAnswerCase()) {
            case QUERYANSWER:
                return queryAnswer(answer.getQueryAnswer());
            case COMPUTEANSWER:
                return computeAnswer(answer.getComputeAnswer());
            case OTHERRESULT:
                return Json.read(answer.getOtherResult()).getValue();
            default:
            case ANSWER_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + answer);
        }
    }

    private Answer queryAnswer(GrpcGrakn.QueryAnswer queryAnswer) {
        ImmutableMap.Builder<Var, Concept> map = ImmutableMap.builder();

        queryAnswer.getQueryAnswerMap().forEach((grpcVar, grpcConcept) -> {
            map.put(Graql.var(grpcVar), conceptReader.concept(grpcConcept));
        });

        return new QueryAnswer(map.build());
    }

    private ComputeQuery.Answer computeAnswer(GrpcGrakn.ComputeAnswer computeAnswerRPC) {
        switch (computeAnswerRPC.getComputeAnswerCase()) {
            case NUMBER:
                try {
                    Number result = NumberFormat.getInstance().parse(computeAnswerRPC.getNumber().getNumber());
                    return new ComputeQueryImpl.AnswerImpl().setNumber(result);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            case PATHS:
                return new ComputeQueryImpl.AnswerImpl().setPaths(paths(computeAnswerRPC.getPaths()));
            case CENTRALITY:
                return new ComputeQueryImpl.AnswerImpl().setCentrality(centrality(computeAnswerRPC.getCentrality()));
            case CLUSTERS:
                return new ComputeQueryImpl.AnswerImpl().setClusters(clusters(computeAnswerRPC.getClusters()));
            case CLUSTERSIZES:
                return new ComputeQueryImpl.AnswerImpl().setClusterSizes(clusterSizes(computeAnswerRPC.getClusterSizes()));
            default:
            case COMPUTEANSWER_NOT_SET:
                throw new IllegalArgumentException("Unexpected " + computeAnswerRPC);
        }
    }

    private List<List<ConceptId>> paths(GrpcGrakn.Paths pathsRPC) {
        List<List<ConceptId>> paths = new ArrayList<>(pathsRPC.getPathsList().size());

        for (GrpcConcept.ConceptIds conceptIds : pathsRPC.getPathsList()) {
            paths.add(
                    conceptIds.getConceptIdsList().stream()
                            .map(conceptIdRPC -> ConceptId.of(conceptIdRPC.getValue()))
                            .collect(Collectors.toList())
            );
        }

        return paths;
    }

    private Map<Long, Set<ConceptId>> centrality(GrpcGrakn.Centrality centralityRPC) {
        Map<Long, Set<ConceptId>> centrality = new HashMap<>();

        for (Map.Entry<Long, GrpcConcept.ConceptIds> entry : centralityRPC.getCentralityMap().entrySet()) {
            centrality.put(
                    entry.getKey(),
                    entry.getValue().getConceptIdsList().stream()
                            .map(conceptIdRPC -> ConceptId.of(conceptIdRPC.getValue()))
                            .collect(Collectors.toSet())
            );
        }

        return centrality;
    }

    private Set<Set<ConceptId>> clusters(GrpcGrakn.Clusters clustersRPC) {
        Set<Set<ConceptId>> clusters = new HashSet<>();

        for (GrpcConcept.ConceptIds conceptIds : clustersRPC.getClustersList()) {
            clusters.add(
                    conceptIds.getConceptIdsList().stream()
                            .map(conceptIdRPC -> ConceptId.of(conceptIdRPC.getValue()))
                            .collect(Collectors.toSet())
            );
        }

        return clusters;
    }

    private Set<Long> clusterSizes(GrpcGrakn.ClusterSizes clusterSizesRPC) {
        return new HashSet<>(clusterSizesRPC.getClusterSizesList());
    }
}

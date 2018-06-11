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

package ai.grakn.rpc;

import ai.grakn.concept.AttributeType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.Label;
import ai.grakn.graql.ComputeQuery;
import ai.grakn.graql.Graql;
import ai.grakn.graql.Pattern;
import ai.grakn.graql.Query;
import ai.grakn.graql.Var;
import ai.grakn.graql.admin.Answer;
import ai.grakn.graql.internal.query.ComputeQueryImpl;
import ai.grakn.graql.internal.query.QueryAnswer;
import ai.grakn.rpc.TxGrpcCommunicator.Response;
import ai.grakn.rpc.generated.GraknGrpc;
import ai.grakn.rpc.generated.GrpcConcept;
import ai.grakn.rpc.generated.GrpcGrakn;
import ai.grakn.rpc.generated.GrpcGrakn.TxRequest;
import ai.grakn.rpc.generated.GrpcGrakn.TxResponse;
import ai.grakn.rpc.generated.GrpcIterator.IteratorId;
import ai.grakn.rpc.util.ConceptMethod;
import ai.grakn.rpc.util.RequestBuilder;
import ai.grakn.rpc.util.ResponseBuilder;
import ai.grakn.rpc.util.ResponseBuilder.ErrorType;
import ai.grakn.rpc.util.TxConceptReader;
import ai.grakn.util.CommonUtil;
import com.google.common.collect.ImmutableMap;
import io.grpc.StatusRuntimeException;
import mjson.Json;

import javax.annotation.Nullable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Communicates with a Grakn gRPC server, translating requests and responses to and from their gRPC representations.
 *
 * <p>
 *     This class is a light abstraction layer over gRPC - it understands how the sequence of calls should execute and
 *     how to translate gRPC objects into Java objects and back.
 * </p>
 *
 * @author Felix Chapman
 */
public class GrpcClient {

    private final TxConceptReader conceptReader;
    private final TxGrpcCommunicator communicator;

    private GrpcClient(TxConceptReader conceptReader, TxGrpcCommunicator communicator) {
        this.conceptReader = conceptReader;
        this.communicator = communicator;
    }

    public static GrpcClient create(TxConceptReader conceptReader, TxGrpcCommunicator communicator) {
        return new GrpcClient(conceptReader, communicator);
    }

    public Iterator<Object> execQuery(Query<?> query) {
        communicator.send(RequestBuilder.execQuery(query.toString(), query.inferring()));

        TxResponse txResponse = responseOrThrow();

        switch (txResponse.getResponseCase()) {
            case ANSWER:
                return Collections.singleton(answer(txResponse.getAnswer())).iterator();
            case DONE:
                return Collections.emptyIterator();
            case ITERATORID:
                IteratorId iteratorId = txResponse.getIteratorId();
                return new ResponseIterator<>(this, iteratorId, response -> answer(response.getAnswer()));
            default:
                throw CommonUtil.unreachableStatement("Unexpected " + txResponse);
        }
    }

    public TxResponse next(IteratorId iteratorId) {
        communicator.send(RequestBuilder.next(iteratorId));
        return responseOrThrow();
    }

    @Nullable
    public <T> T runConceptMethod(ConceptId id, ConceptMethod<T> conceptMethod) {
        communicator.send(RequestBuilder.runConceptMethod(id, conceptMethod));
        return conceptMethod.readResponse(conceptReader, this, responseOrThrow());
    }

    public Stream<? extends Concept> getAttributesByValue(Object value) {
        communicator.send(RequestBuilder.getAttributesByValue(value));
        IteratorId iteratorId = responseOrThrow().getIteratorId();
        Iterable<Concept> iterable = () -> new ResponseIterator<>(this, iteratorId, response -> conceptReader.concept(response.getConcept()));

        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private TxResponse responseOrThrow() {
        Response response;

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
        ErrorType errorType = error.getTrailers().get(ResponseBuilder.ErrorType.KEY);

        if (errorType != null) return errorType.toException(error.getStatus().getDescription());
        else return error;
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
